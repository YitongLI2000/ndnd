package fw

import (
	"bytes"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/named-data/ndnd/fw/core"
	"github.com/named-data/ndnd/fw/defn"
	"github.com/named-data/ndnd/fw/dispatch"
	"github.com/named-data/ndnd/fw/table"
	"github.com/named-data/ndnd/std/datapacket"
	enc "github.com/named-data/ndnd/std/encoding"
	spec "github.com/named-data/ndnd/std/ndn/spec_2022"
)

// InterestQueueItem represents an item in the per-prefix Interest queue.
type InterestQueueItem struct {
	Packet   *defn.Pkt
	PitEntry table.PitEntry
	InFace   uint64
}

// hopSnapshot is an immutable copy of FIB nexthop metadata used by PD state.
type hopSnapshot struct {
	Nexthop uint64
	Cost    uint64
}

// DtRateControlParams keeps all DT face-level rate control hyperparameters in one place.
// Units:
// - Queue parameters are in "queue signal" units (to be defined by Step 1 integration).
// - Rates are in Interests/sec.
type DtRateControlParams struct {
	MaxFwdQueueSize   float64
	FwdQueueAlpha     float64
	FwdQueueBeta      float64
	FwdMDFactor       float64
	FwdRPFactor       float64
	FwdGDFactor       float64
	FwdCIFactor       float64
	FwdSlopeThreshold float64
	MaxBWSafetyRatio  float64
	MinBWSafetyRatio  float64
	FwdMinRate        float64
	FwdMaxRate        float64
}

const (
	dtBaseThroughputWindow  = 20 * time.Millisecond
	dtMinThroughputWindow   = 1 * time.Millisecond
	dtMaxExpectedPps        = 100000.0
	dtWindowSafetyFactor    = 2.0
	dtSlopeTauRatio         = 1.0 / 3.0
	dtControlRttInitSamples = 10
	// DT per-face control period = dtControlPeriodRttScale * avg bootstrap RTT.
	dtControlPeriodRttScale = 1.0
	dtControlPeriodMin      = 2 * time.Millisecond
	dtControlPeriodMax      = 500 * time.Millisecond
)

var dtRateControlParams = DtRateControlParams{
	MaxFwdQueueSize:   20.0,
	FwdQueueAlpha:     0.5,
	FwdQueueBeta:      0.8,
	FwdMDFactor:       0.9,
	FwdRPFactor:       1.05,
	FwdGDFactor:       0.95,
	FwdCIFactor:       1.02,
	FwdSlopeThreshold: 0.1,
	MaxBWSafetyRatio:  1.5,
	MinBWSafetyRatio:  0.5,
	FwdMinRate:        0.005,
	FwdMaxRate:        0.0, // 0 means "disabled"
}

var (
	dtThroughputWindowDur  = dtBaseThroughputWindow
	dtMaxSamplesPerFace    = int(dtMaxExpectedPps * dtBaseThroughputWindow.Seconds() * dtWindowSafetyFactor)
	dtDataPacketSizeBytes  = 0
	dtEstimatorTunableOnce sync.Once
)

type DtRateControlAction int

const (
	DtActionEmergencyDecrease DtRateControlAction = iota + 1
	DtActionHoldDraining
	DtActionGentleDecrease
	DtActionCautiousIncrease
	DtActionAggressiveProbe
	DtActionBWCapped
	DtActionBWBoosted
	DtActionGlobalMinCapped
	DtActionGlobalMaxCapped
)

func dtRateActionString(action DtRateControlAction) string {
	switch action {
	case DtActionEmergencyDecrease:
		return "emergency-decrease"
	case DtActionHoldDraining:
		return "hold-draining"
	case DtActionGentleDecrease:
		return "gentle-decrease"
	case DtActionCautiousIncrease:
		return "cautious-increase"
	case DtActionAggressiveProbe:
		return "aggressive-probe"
	case DtActionBWCapped:
		return "bw-capped"
	case DtActionBWBoosted:
		return "bw-boosted"
	case DtActionGlobalMinCapped:
		return "global-min-capped"
	case DtActionGlobalMaxCapped:
		return "global-max-capped"
	default:
		return "unknown"
	}
}

type DtUpstreamSample struct {
	Timestamp   time.Time
	SizeBytes   int
	InterestQsf float64
	DataQsf     float64
}

type dtRttBootstrapState struct {
	mu     sync.Mutex
	count  int
	sum    time.Duration
	ready  bool
	period time.Duration
}

type dtPrefixInterestSignal struct {
	InterestQ     float64
	InterestSlope float64
	DataQ         float64
	DataSlope     float64
	BWPps         float64
	SampleCount   int
	LastUpdated   time.Time
}

type dtFaceInterestSignalState struct {
	mu     sync.RWMutex
	byPref map[string]dtPrefixInterestSignal
}

type dtFaceControlState struct {
	mu        sync.Mutex
	activated bool
	period    time.Duration
	nextRun   time.Time
	rate      float64
	action    DtRateControlAction
}

// PathDiscoveryInfo holds all state for a specific prefix (Namespace).
type PathDiscoveryInfo struct {
	mu sync.Mutex

	Prefix        string
	IsInitialized bool

	// ---- Hop-tier grouping ----
	TierList     [][]hopSnapshot
	TierCostList []uint64
	CurrentTier  int

	// ---- Path Discovery State ----
	ProbeIndex           int
	WaitList             [][]hopSnapshot
	ProbedUpstreams      []uint64
	ProbedSchedulerIndex int

	// ---- Convergence State ----
	ConvergedUpstreamTier map[uint64]bool
	ConvergedUpstreamNode map[uint64]bool
	IsNodeConverged       bool
	ValidUpstreams        map[uint64]bool
	IsInvalidNode         bool
	PrefixPdSendRate      float64

	// ---- Frozen / Race Condition Handling ----
	IsPdFrozen         bool
	FrozenFaces        map[uint64]bool
	FrozenRecoverFaces map[uint64]bool

	// ---- Queue Management ----
	// Keep PD and DT queues isolated even though they are both per-prefix.
	PdInterestQueue []*InterestQueueItem
	TxInterestQueue []*InterestQueueItem

	// ---- Scheduler ----
	Ticker     *time.Ticker
	StopTicker chan struct{}

	// ---- Data Transmission ----
	ActiveFaces     []uint64
	ActiveFaceIndex int
	// Placeholder states for per-upstream DT congestion control.
	// Update logic of these metrics is handled by separate steps.
	UpstreamEstimatedBW      map[uint64]float64
	UpstreamQsfInterest      map[uint64]float64
	UpstreamQsfData          map[uint64]float64
	UpstreamQsfInterestSlope map[uint64]float64
	UpstreamQsfDataSlope     map[uint64]float64
	UpstreamSamples          map[uint64][]DtUpstreamSample

	// ---- Round-Robin Counter (for simple forwarding) (ATOMIC) ----
	RoundRobinCounter uint64
}

// Shared state across all Multipath strategy instances (all threads)
// This ensures that PathDiscoveryInfo is shared across threads, not duplicated per thread
var multipathPdState sync.Map
var dtRttBootstrapByFace sync.Map
var dtFaceInterestSignalByFace sync.Map
var dtFaceControlByFace sync.Map

// Multipath Strategy Struct
type Multipath struct {
	StrategyBase
}

func init() {
	strategyInit = append(strategyInit, func() Strategy { return &Multipath{} })
	StrategyVersions["multipath"] = []uint64{1}
	initDtEstimatorTunables()
}

func (s *Multipath) Instantiate(fwThread *Thread) {
	s.NewStrategyBase(fwThread, "multipath", 1)
}

// Helper to get or create the state for a prefix
// THREAD SAFETY: Uses package-level multipathPdState shared across all threads
func (s *Multipath) getPathDiscoveryInfo(prefix string) *PathDiscoveryInfo {
	val, loaded := multipathPdState.LoadOrStore(prefix, &PathDiscoveryInfo{
		Prefix:                   prefix,
		ConvergedUpstreamTier:    make(map[uint64]bool),
		ConvergedUpstreamNode:    make(map[uint64]bool),
		ValidUpstreams:           make(map[uint64]bool),
		FrozenFaces:              make(map[uint64]bool),
		FrozenRecoverFaces:       make(map[uint64]bool),
		PdInterestQueue:          make([]*InterestQueueItem, 0),
		TxInterestQueue:          make([]*InterestQueueItem, 0),
		UpstreamEstimatedBW:      make(map[uint64]float64),
		UpstreamQsfInterest:      make(map[uint64]float64),
		UpstreamQsfData:          make(map[uint64]float64),
		UpstreamQsfInterestSlope: make(map[uint64]float64),
		UpstreamQsfDataSlope:     make(map[uint64]float64),
		UpstreamSamples:          make(map[uint64][]DtUpstreamSample),
		StopTicker:               make(chan struct{}),
	})

	info := val.(*PathDiscoveryInfo)

	if !loaded {
		core.Log.Info(s, "Created new PathDiscoveryInfo for prefix", prefix)
	}

	return info
}

func (s *Multipath) AfterContentStoreHit(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
	s.SendData(packet, pitEntry, inFace, 0)
}

// AfterReceiveData implements the Data Pipeline logic
func (s *Multipath) AfterReceiveData(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
	// 1. Identify Data Type ("pd" or "dt")
	// Assumption: Name format is /<prefix>/<node>/<type>/...
	var dataType string
	if len(packet.Name) >= 3 {
		dataType = packet.Name[2].String()
	}

	switch dataType {
	case "pd":
		s.handlePathDiscoveryData(packet, pitEntry, inFace)
	case "dt":
		s.handleTransmissionData(packet, pitEntry, inFace)
	default:
		// Fallback: BestRoute Logic
		// Forwards a received Data packet to every face recorded in the PIT entry
		core.Log.Trace(s, "AfterReceiveData (BestRoute Fallback)", "name", packet.Name, "inrecords", len(pitEntry.InRecords()))
		for faceID := range pitEntry.InRecords() {
			// core.Log.Trace(s, "Forwarding Data", "name", packet.Name, "faceid", faceID)
			s.SendData(packet, pitEntry, faceID, inFace)
		}
	}
}

// AfterReceiveInterest implements the Interest Pipeline logic
func (s *Multipath) AfterReceiveInterest(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
	nexthops []*table.FibNextHopEntry,
) {
	// 1. Identify Interest Type
	// Assumption: Name format is /<prefix>/<node>/<type>/.../<probeIdx>/<seq>
	var interestType string
	if len(packet.Name) >= 3 {
		interestType = packet.Name[2].String()
	}

	switch interestType {
	case "pd":
		s.handlePathDiscoveryInterest(packet, pitEntry, inFace, nexthops)
	case "dt":
		s.handleDataTransmissionInterest(packet, pitEntry, inFace, nexthops)
	default:
		// Fallback: BestRoute Logic
		s.handleBestRouteInterest(packet, pitEntry, inFace, nexthops)
	}
}

// handleBestRouteInterest implements the logic from bestroute.go
func (s *Multipath) handleBestRouteInterest(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
	nexthops []*table.FibNextHopEntry,
) {
	if len(nexthops) == 0 {
		core.Log.Debug(s, "No nexthop found - DROP", "name", packet.Name)
		return
	}

	// Sort nexthops by cost and send to best-possible nexthop
	// We create a copy to avoid modifying the slice passed by the forwarder if it's shared (though usually it's safe to sort)
	sortedHops := make([]*table.FibNextHopEntry, len(nexthops))
	copy(sortedHops, nexthops)
	sort.Slice(sortedHops, func(i, j int) bool { return sortedHops[i].Cost < sortedHops[j].Cost })

	now := time.Now()
	for pass := range 2 {
		for _, nh := range sortedHops {
			// In the first pass, skip hops that already have a out record
			if pass == 0 {
				if oR := pitEntry.OutRecords()[nh.Nexthop]; oR != nil {
					// Suppress retransmissions of the same Interest within suppression time
					if oR.LatestTimestamp.Add(BestRouteSuppressionTime).After(now) {
						core.Log.Debug(s, "Suppressed Interest - DROP", "name", packet.Name)
						return
					}

					// If an out record exists, skip this hop
					continue
				}
			}

			// For the second pass, we should ideally use the least recently tried hop.
			// But then we need to resort the list - this is just faster for now.
			// In densely connected networks, this is not a big deal.

			core.Log.Trace(s, "Forwarding Interest (BestRoute)", "name", packet.Name, "faceid", nh.Nexthop)
			if sent := s.SendInterest(packet, pitEntry, nh.Nexthop, inFace); sent {
				return
			}
		}
	}

	core.Log.Debug(s, "No usable nexthop for Interest - DROP", "name", packet.Name)
}

// handleDataTransmissionInterest keeps the previous logic for non-pd packets
func (s *Multipath) handleDataTransmissionInterest(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
	nexthops []*table.FibNextHopEntry,
) {
	// Extract sequence number from interest name and log every 200 packets
	interestName := packet.Name.String()
	seqNum := extractSeqNum(interestName)
	if seqNum >= 0 && seqNum%200 == 0 {
		core.Log.Info(s, "Interest Forwarded", "name", interestName, "seq", seqNum)
	}

	if len(nexthops) == 0 {
		core.Log.Warn(s, "No nexthops available for", packet.Name)
		return
	}

	// 1. Sort Nexthops
	sortedHops := make([]*table.FibNextHopEntry, len(nexthops))
	copy(sortedHops, nexthops)
	sort.Slice(sortedHops, func(i, j int) bool {
		return sortedHops[i].Nexthop < sortedHops[j].Nexthop
	})
	candidateFaces := make([]uint64, 0, len(sortedHops))
	for _, nh := range sortedHops {
		candidateFaces = append(candidateFaces, nh.Nexthop)
	}

	// 2. Identify the Flow/Prefix
	var prefixKey string
	if len(packet.Name) > 0 {
		prefixKey = packet.Name[0].String()
	} else {
		prefixKey = "/"
	}

	// 3. Get Info
	info := s.getPathDiscoveryInfo(prefixKey)
	info.mu.Lock()
	s.ensureDtControlActivated(prefixKey, candidateFaces, time.Now())

	// 4. Queue incoming Interest into the DT-specific per-prefix queue
	info.TxInterestQueue = append(info.TxInterestQueue, &InterestQueueItem{
		Packet:   packet,
		PitEntry: pitEntry,
		InFace:   inFace,
	})

	// 5. Dequeue immediately (placeholder behavior before rate shaping)
	queuedItem := info.TxInterestQueue[0]
	info.TxInterestQueue[0] = nil // release reference
	info.TxInterestQueue = info.TxInterestQueue[1:]

	// 6. Atomic Round-Robin Selection
	// THREAD SAFETY: Atomic increment is safe without mutex.
	currentCount := atomic.AddUint64(&info.RoundRobinCounter, 1) - 1
	selectedIndex := currentCount % uint64(len(sortedHops))
	targetFace := sortedHops[selectedIndex].Nexthop
	info.mu.Unlock()

	// 7. Send dequeued Interest immediately
	s.SendInterest(queuedItem.Packet, queuedItem.PitEntry, targetFace, queuedItem.InFace)
}

// handlePathDiscoveryInterest implements the ported C++ logic for PD mode
// THREAD SAFETY: Entire function logic is protected by info.mu.
func (s *Multipath) handlePathDiscoveryInterest(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
	nexthops []*table.FibNextHopEntry,
) {
	// Name format assumption based on C++: /<prefix>/<srcNode>/pd/.../<probeIdx>/<seq>
	// C++: probeIdx is at get(-2)
	if len(packet.Name) < 2 {
		core.Log.Warn(s, "handlePathDiscoveryInterest: short name", "name", packet.Name)
		return
	}

	prefix := packet.Name[0].String()
	srcNode := packet.Name[1].String()

	// Parse Probe Index
	probeIdxStr := packet.Name[len(packet.Name)-2].String()
	probeIdx, err := strconv.Atoi(probeIdxStr)
	if err != nil {
		core.Log.Error(s, "Invalid probe index in PD Interest", "name", packet.Name)
		return
	}

	core.Log.Debug(s, "Received PD Interest", "name", packet.Name, "inFace", inFace, "probeIdx", probeIdx)

	info := s.getPathDiscoveryInfo(prefix)
	info.mu.Lock()
	defer info.mu.Unlock()

	// 1. Check Irrelevant Node (Pro/Con logic) & Invalid Node Flag
	myNodeName := s.StrategyNodeName.String()

	isPro := strings.HasPrefix(myNodeName, "/pro")
	isCon := strings.HasPrefix(myNodeName, "/con")

	// Normalize: myNodeName has leading "/" (e.g., "/pro0"), but prefix/srcNode from packet.Name don't (e.g., "pro0app", "con0app")
	// Remove leading "/" from myNodeName for comparison
	myNodeNameWithoutSlash := strings.TrimPrefix(myNodeName, "/")

	// Also normalize prefix/srcNode in case they have leading "/" (defensive)
	prefixNormalized := strings.TrimPrefix(prefix, "/")
	srcNodeNormalized := strings.TrimPrefix(srcNode, "/")

	// For producer: prefix is like "pro0app", node name is like "/pro0" -> "pro0"
	// We check if prefix starts with the node name without "/" (e.g., "pro0app" starts with "pro0")
	// For consumer: srcNode is like "con0app", node name is like "/con0" -> "con0"
	// We check if srcNode starts with the node name without "/" (e.g., "con0app" starts with "con0")
	var isRelevant bool
	if isPro {
		// Producer: prefixNormalized should start with myNodeNameWithoutSlash (e.g., "pro0app" starts with "pro0")
		isRelevant = strings.HasPrefix(prefixNormalized, myNodeNameWithoutSlash)
	} else if isCon {
		// Consumer: srcNodeNormalized should start with myNodeNameWithoutSlash (e.g., "con0app" starts with "con0")
		isRelevant = strings.HasPrefix(srcNodeNormalized, myNodeNameWithoutSlash)
	} else {
		// Unknown node type, treat as irrelevant
		isRelevant = false
	}

	if !isRelevant {
		// Irrelevant NFD
		core.Log.Debug(s, "Nack irrelevant Interest", "name", packet.Name, "myNode", myNodeName, "prefix", prefix, "srcNode", srcNode)
		info.IsInvalidNode = true
		s.SendNack(packet, pitEntry, inFace, inFace, spec.NackReasonNoRoute)
		return
	} else if info.IsInvalidNode {
		core.Log.Debug(s, "Node marked invalid, sending Nack", "name", packet.Name)
		s.SendNack(packet, pitEntry, inFace, inFace, spec.NackReasonNoRoute)
		return
	}

	// 2. Initialization
	if !info.IsInitialized {
		core.Log.Debug(s, "Initializing Path Discovery", "prefix", prefix, "probeIdx", probeIdx)
		info.IsInitialized = true
		info.ProbeIndex = probeIdx

		s.TierGrouping(info, s.snapshotNexthops(nexthops))
		s.StartPathDiscovery(info)
	}

	// 3. Tier Probing Logic
	if !info.IsNodeConverged {
		if probeIdx > info.ProbeIndex {
			// Refresh tier snapshots from the latest FIB view before advancing stage.
			s.refreshPathDiscoverySnapshot(info, nexthops)

			// Lock check
			if info.CurrentTier >= len(info.WaitList)-1 {
				core.Log.Error(s, "Reach the end of wait list, probe idx beyond range", "prefix", prefix)
				return // Stop processing
			} else {
				info.ProbeIndex = probeIdx
				s.ProbeNextTier(info)
			}
		} else if probeIdx == info.ProbeIndex {
			// Still in current tier probing
		} else {
			// probeIdx < info.ProbeIndex: Wrong case
			core.Log.Error(s, "Received old probeIdx", "received", probeIdx, "current", info.ProbeIndex, "prefix", prefix)
			return
		}

		// 4. Face Management (Frozen vs Removal)
		isProbed := false
		for _, face := range info.ProbedUpstreams {
			if face == inFace {
				isProbed = true
				break
			}
		}

		if isProbed {
			// Find tier index of ingress face
			tierIndex := -1
			for idx, tier := range info.WaitList {
				for _, hop := range tier {
					if hop.Nexthop == inFace {
						tierIndex = idx
						break
					}
				}
				if tierIndex != -1 {
					break
				}
			}

			if tierIndex != -1 {
				if !info.IsPdFrozen {
					info.IsPdFrozen = true
					// Add all faces in this tier to frozen lists
					for _, hop := range info.WaitList[tierIndex] {
						info.FrozenFaces[hop.Nexthop] = true
						info.FrozenRecoverFaces[hop.Nexthop] = true
					}
					core.Log.Debug(s, "Frozen removal later", "prefix", prefix, "triggerFace", inFace, "tier", tierIndex)
				}
			} else {
				core.Log.Error(s, "Cannot find ingress face in wait list", "face", inFace, "prefix", prefix)
				return
			}
		} else {
			// Execute Removal Immediately
			core.Log.Debug(s, "Exclude ingress face from valid/wait/converged/probed", "face", inFace, "prefix", prefix)

			delete(info.ValidUpstreams, inFace)

			// Remove from WaitList
			s.removeFaceFromWaitList(info, inFace)

			delete(info.ConvergedUpstreamTier, inFace)
			delete(info.ConvergedUpstreamNode, inFace)

			// Remove from ProbedUpstreams
			s.removeFaceFromProbedUpstreams(info, inFace)
		}

		// 5. Check Skip Tier
		if s.isCurrentTierAllInvalid(info) {
			s.skipTier(info)
		}

		// 6. Check No Route
		if len(info.ValidUpstreams) == 0 {
			core.Log.Debug(s, "No valid upstreams left, marking node invalid", "prefix", prefix)
			info.IsInvalidNode = true
			s.SendNack(packet, pitEntry, inFace, inFace, spec.NackReasonNoRoute)
			return
		}
	}

	// 7. Forwarding (Round-Robin directly, no queue/scheduler)
	if len(info.ProbedUpstreams) == 0 {
		// No faces to probe
		core.Log.Error(s, "No valid faces to probe in pd", "name", packet.Name)
		return
	}

	// Use ProbedSchedulerIndex for Round-Robin
	idx := info.ProbedSchedulerIndex
	if idx >= len(info.ProbedUpstreams) {
		idx = 0
	}

	outFace := info.ProbedUpstreams[idx]

	// Advance index
	info.ProbedSchedulerIndex = (idx + 1) % len(info.ProbedUpstreams)

	// Send Interest
	s.SendInterest(packet, pitEntry, outFace, inFace)

	// 8. Frozen Flag Handling (Ported from C++ PathDiscoveryScheduler)
	// THREAD SAFETY: Protected by info.mu
	if info.IsPdFrozen {
		// If the face we just sent to was in the frozen set, remove it.
		if info.FrozenFaces[outFace] {
			delete(info.FrozenFaces, outFace)

			// If no more faces are frozen, we can proceed with the delayed removal
			if len(info.FrozenFaces) == 0 {
				info.IsPdFrozen = false
				core.Log.Debug(s, "Unset frozen flag", "prefix", prefix)

				// Process the recovery list: these are faces that needed to be removed
				// but were held back because the state was frozen.
				for face := range info.FrozenRecoverFaces {
					// Perform the delayed removals
					core.Log.Debug(s, "Processing frozen recovery removal", "face", face, "prefix", prefix)
					delete(info.ValidUpstreams, face)
					s.removeFaceFromWaitList(info, face)
					delete(info.ConvergedUpstreamTier, face)
					delete(info.ConvergedUpstreamNode, face)
					s.removeFaceFromProbedUpstreams(info, face)
				}

				// Clear the recovery map for the next time
				info.FrozenRecoverFaces = make(map[uint64]bool)
			}
		}
	}
}

// ---- Helper Methods for PD Logic ----

func (s *Multipath) snapshotNexthops(nexthops []*table.FibNextHopEntry) []hopSnapshot {
	snapshot := make([]hopSnapshot, 0, len(nexthops))
	for _, nh := range nexthops {
		if nh == nil {
			continue
		}
		snapshot = append(snapshot, hopSnapshot{
			Nexthop: nh.Nexthop,
			Cost:    nh.Cost,
		})
	}
	return snapshot
}

func groupHopSnapshotsByCost(hops []hopSnapshot) ([][]hopSnapshot, []uint64) {
	costToFaces := make(map[uint64][]hopSnapshot)
	costs := make([]uint64, 0)

	for _, hop := range hops {
		if _, exists := costToFaces[hop.Cost]; !exists {
			costs = append(costs, hop.Cost)
		}
		costToFaces[hop.Cost] = append(costToFaces[hop.Cost], hop)
	}

	sort.Slice(costs, func(i, j int) bool { return costs[i] < costs[j] })

	tierList := make([][]hopSnapshot, 0, len(costs))
	costList := make([]uint64, 0, len(costs))
	for _, cost := range costs {
		tier := make([]hopSnapshot, len(costToFaces[cost]))
		copy(tier, costToFaces[cost])
		tierList = append(tierList, tier)
		costList = append(costList, cost)
	}

	return tierList, costList
}

func cloneTierList(tiers [][]hopSnapshot) [][]hopSnapshot {
	cloned := make([][]hopSnapshot, len(tiers))
	for i, tier := range tiers {
		copiedTier := make([]hopSnapshot, len(tier))
		copy(copiedTier, tier)
		cloned[i] = copiedTier
	}
	return cloned
}

func (s *Multipath) refreshPathDiscoverySnapshot(info *PathDiscoveryInfo, nexthops []*table.FibNextHopEntry) {
	latest := s.snapshotNexthops(nexthops)
	if len(latest) == 0 {
		return
	}

	latestTiers, latestCosts := groupHopSnapshotsByCost(latest)
	latestFaces := make(map[uint64]bool, len(latest))
	for _, hop := range latest {
		latestFaces[hop.Nexthop] = true
	}

	// Remove disappeared faces from all state sets.
	for face := range info.ValidUpstreams {
		if latestFaces[face] {
			continue
		}
		delete(info.ValidUpstreams, face)
		delete(info.ConvergedUpstreamTier, face)
		delete(info.ConvergedUpstreamNode, face)
		delete(info.FrozenFaces, face)
		delete(info.FrozenRecoverFaces, face)
		s.removeFaceFromProbedUpstreams(info, face)
	}

	// Rebuild tiers from fresh snapshot but preserve current valid-face decisions.
	filteredTiers := make([][]hopSnapshot, len(latestTiers))
	for i, tier := range latestTiers {
		filtered := make([]hopSnapshot, 0, len(tier))
		for _, hop := range tier {
			if info.ValidUpstreams[hop.Nexthop] {
				filtered = append(filtered, hop)
			}
		}
		filteredTiers[i] = filtered
	}

	info.TierList = filteredTiers
	info.WaitList = cloneTierList(filteredTiers)
	info.TierCostList = latestCosts

	// Keep only probed faces that still exist and are still valid.
	filteredProbed := make([]uint64, 0, len(info.ProbedUpstreams))
	seen := make(map[uint64]bool, len(info.ProbedUpstreams))
	for _, face := range info.ProbedUpstreams {
		if !latestFaces[face] || !info.ValidUpstreams[face] || seen[face] {
			continue
		}
		filteredProbed = append(filteredProbed, face)
		seen[face] = true
	}
	info.ProbedUpstreams = filteredProbed
	if len(info.ProbedUpstreams) == 0 || info.ProbedSchedulerIndex >= len(info.ProbedUpstreams) {
		info.ProbedSchedulerIndex = 0
	}

	// Keep CurrentTier in range after snapshot rebuild.
	if len(info.WaitList) == 0 {
		info.CurrentTier = 0
		return
	}
	if info.CurrentTier >= len(info.WaitList) {
		info.CurrentTier = len(info.WaitList) - 1
	}
}

func (s *Multipath) TierGrouping(info *PathDiscoveryInfo, nexthops []hopSnapshot) {
	info.TierList, info.TierCostList = groupHopSnapshotsByCost(nexthops)
	for i, tier := range info.TierList {
		core.Log.Debug(s, "TierGrouping", "cost", info.TierCostList[i], "facesCount", len(tier))
	}

	// Initialize WaitList as a deep copy of TierList.
	info.WaitList = cloneTierList(info.TierList)

	info.CurrentTier = 0

	// Log grouping results summary
	totalFaces := 0
	for _, tier := range info.TierList {
		totalFaces += len(tier)
	}
	core.Log.Debug(s, "TierGrouping completed",
		"prefix", info.Prefix,
		"totalTiers", len(info.TierList),
		"totalFaces", totalFaces,
		"tierCosts", info.TierCostList)
}

func (s *Multipath) StartPathDiscovery(info *PathDiscoveryInfo) {
	// 1. Initialize valid upstream as all nexthops
	info.ValidUpstreams = make(map[uint64]bool)
	info.ConvergedUpstreamNode = make(map[uint64]bool)

	// Flatten TierList to get all nexthops initially
	for _, tier := range info.TierList {
		for _, nh := range tier {
			info.ValidUpstreams[nh.Nexthop] = true
			info.ConvergedUpstreamNode[nh.Nexthop] = true
		}
	}

	// 2. Start with tier 0
	info.CurrentTier = 0

	// 3. Remove unnecessary tiers (Cost >= 2x First Tier Cost)
	if len(info.TierCostList) > 0 {
		firstTierCost := info.TierCostList[0]
		thresholdCost := firstTierCost * 2

		core.Log.Debug(s, "StartPathDiscovery", "firstTierCost", firstTierCost, "thresholdCost", thresholdCost)

		// Process from highest to lowest
		for i := len(info.TierCostList) - 1; i > 0; i-- {
			if info.TierCostList[i] >= thresholdCost {
				core.Log.Debug(s, "Removing tier due to high cost", "tier", i, "cost", info.TierCostList[i])
				facesToRemove := info.WaitList[i]

				for _, hop := range facesToRemove {
					delete(info.ValidUpstreams, hop.Nexthop)
					delete(info.ConvergedUpstreamNode, hop.Nexthop)
				}

				// Clear tier
				info.WaitList[i] = []hopSnapshot{}
			}
		}
	}

	// 4. Start probing current tier
	s.ProbeTier(info, info.CurrentTier)

	// 5. Reset scheduler index
	info.ProbedSchedulerIndex = 0
}

func (s *Multipath) ProbeTier(info *PathDiscoveryInfo, tierIndex int) {
	if tierIndex >= len(info.WaitList) {
		core.Log.Error(s, "ProbeTier: tier index out of range", "tierIndex", tierIndex, "waitListLen", len(info.WaitList))
		return
	}

	core.Log.Debug(s, "Probing tier", "tierIndex", tierIndex, "prefix", info.Prefix)

	// Add faces in this tier to ProbedUpstreams
	for _, hop := range info.WaitList[tierIndex] {
		// Add to ProbedUpstreams if not exists
		exists := false
		for _, face := range info.ProbedUpstreams {
			if face == hop.Nexthop {
				exists = true
				break
			}
		}
		if !exists {
			info.ProbedUpstreams = append(info.ProbedUpstreams, hop.Nexthop)
		}

		info.ConvergedUpstreamTier[hop.Nexthop] = true
		core.Log.Debug(s, "Start probing face", "face", hop.Nexthop)
	}

	// Node convergence for producer NFD
	// Check if prefix starts with node name (handling "/" prefix correctly)
	// Example: prefix "/pro0app" should match node name "/pro0"
	// Normalize both by removing leading "/" and check if prefix starts with node name
	if !info.IsNodeConverged {
		myNodeName := s.StrategyNodeName.String()
		myNodeNameNormalized := strings.TrimPrefix(myNodeName, "/")
		prefixNormalized := strings.TrimPrefix(info.Prefix, "/")
		if strings.HasPrefix(prefixNormalized, myNodeNameNormalized) {
			core.Log.Debug(s, "Node is designated pro NFD, node converged", "prefix", info.Prefix, "nodeName", myNodeName)
			s.localNodeConverged(info)
		}
	}
}

func (s *Multipath) ProbeNextTier(info *PathDiscoveryInfo) {
	nextTier := info.CurrentTier + 1
	info.CurrentTier = nextTier
	s.ProbeTier(info, nextTier)
}

func (s *Multipath) BeforeSatisfyInterest(pitEntry table.PitEntry, inFace uint64) {}

// AfterReceiveLoopedInterest handles looped interests (when a duplicate Interest is detected)
// Aligned with C++ logic in multipath-strategy.cpp
// THREAD SAFETY: Stateless (safe).
func (s *Multipath) AfterReceiveLoopedInterest(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
	// 1. Identify Interest Type ("pd" or "dt")
	// Assumption: Name format is /<prefix>/<node>/<type>/...
	if len(packet.Name) < 3 {
		core.Log.Warn(s, "AfterReceiveLoopedInterest: short name", "name", packet.Name)
		return
	}

	interestType := packet.Name[2].String()

	//! Send NACK with "NONE" reason
	// Either using this way or sendnack as defined in strategy would work
	switch interestType {
	case "pd":
		// In Go, we use the strategy helper to send the NACK
		s.SendNack(packet, pitEntry, inFace, inFace, spec.NackReasonNone)
	default:
		// In Go, we just log the error as requested
		core.Log.Error(s, "[ERROR] Received looped interest in data transmission stage! Please check.", "name", packet.Name)
		return
	}
}

// AfterReceiveNack handles NACKs
func (s *Multipath) AfterReceiveNack(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64, nackReason uint64) {
	// 1. Identify Interest Type ("pd" or "dt")
	// Assumption: Name format is /<prefix>/<node>/<type>/...
	var interestType string
	if len(packet.Name) >= 3 {
		interestType = packet.Name[2].String()
	}

	switch interestType {
	case "pd":
		s.handlePathDiscoveryNack(packet, pitEntry, inFace, nackReason)
	case "dt":
		s.handleTransmissionNack(packet, pitEntry, inFace, nackReason)
	default:
		// Fallback: BestRoute Logic (Default Nack Forwarding)
		// Since bestroute.go provided doesn't specify Nack handling, we use default forwarding
		core.Log.Trace(s, "AfterReceiveNack (BestRoute Fallback)", "name", packet.Name, "reason", nackReason)
		s.forwardNackDefault(packet, pitEntry, inFace, nackReason)
	}
}

// forwardNackDefault forwards NACK to all downstream faces (standard NFD behavior)
func (s *Multipath) forwardNackDefault(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64, nackReason uint64) {
	// Forward NACK to all downstream faces (in-records) except the incoming face
	for faceID := range pitEntry.InRecords() {
		if faceID != inFace {
			s.SendNack(packet, pitEntry, faceID, inFace, nackReason)
		}
	}
}

// handlePathDiscoveryNack processes NACKs for path discovery Interests
// THREAD SAFETY: Protected by info.mu
func (s *Multipath) handlePathDiscoveryNack(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64, nackReason uint64) {
	// 1. Lookup FIB to get Prefix
	prefix := "/"
	if len(packet.Name) > 0 {
		prefix = packet.Name[0].String()
	}

	info := s.getPathDiscoveryInfo(prefix)
	info.mu.Lock()
	defer info.mu.Unlock()

	core.Log.Debug(s, "Path Discovery NACK received", "name", packet.Name, "reason", nackReason, "inFace", inFace, "prefix", prefix)

	if nackReason == spec.NackReasonNoRoute {
		// Check invalid node flag
		if info.IsInvalidNode {
			s.forwardNackDefault(packet, pitEntry, inFace, nackReason)
			return
		}

		// Exclude invalid upstream faces if not frozen
		if !info.IsPdFrozen {
			core.Log.Debug(s, "Exclude ingress face from valid/wait/converged/probed due to NACK", "face", inFace, "prefix", prefix)
			// 1. Remove from ValidUpstreams
			delete(info.ValidUpstreams, inFace)

			// 2. Remove from WaitList (using helper)
			s.removeFaceFromWaitList(info, inFace)

			// 3. Remove from ConvergedUpstreamTier
			delete(info.ConvergedUpstreamTier, inFace)

			// 4. Remove from ConvergedUpstreamNode
			delete(info.ConvergedUpstreamNode, inFace)

			// 5. Remove from ProbedUpstreams (using helper)
			s.removeFaceFromProbedUpstreams(info, inFace)
		} else {
			core.Log.Debug(s, "Frozen flag is set, ignore nack", "face", inFace, "prefix", prefix)
		}

		// Check skip tier
		// If all faces in the current tier are invalid, we skip the rest of the tiers and converge
		if s.isCurrentTierAllInvalid(info) {
			s.skipTier(info)
		}

		// Check whether there's no valid upstream left
		if len(info.ValidUpstreams) == 0 {
			core.Log.Debug(s, "Invalid node, send nack to disable this node", "prefix", prefix)
			info.IsInvalidNode = true
			s.forwardNackDefault(packet, pitEntry, inFace, spec.NackReasonNoRoute)
			return
		}

		// Send NACK with "NONE" reason
		// This suppresses the NO_ROUTE NACK to downstream if we still have valid options
		s.forwardNackDefault(packet, pitEntry, inFace, spec.NackReasonNone)
		return

	} else if nackReason == spec.NackReasonNone {
		// Propagate NONE
		s.forwardNackDefault(packet, pitEntry, inFace, spec.NackReasonNone)
	}
}

// handleTransmissionNack processes NACKs for data transmission Interests
// THREAD SAFETY: Protected by info.mu
func (s *Multipath) handleTransmissionNack(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64, nackReason uint64) {
	// 1. Lookup FIB to get Prefix
	prefix := "/"
	if len(packet.Name) > 0 {
		prefix = packet.Name[0].String()
	}

	info := s.getPathDiscoveryInfo(prefix)
	info.mu.Lock()
	defer info.mu.Unlock()

	core.Log.Info(s, "Data Transmission NACK received", "name", packet.Name, "reason", nackReason, "inFace", inFace, "prefix", prefix)

	// 2. Handle NACK based on reason
	switch nackReason {
	// case spec.NackReasonCongestion:
	// 	// Congestion NACK - mark face but don't remove from active faces immediately
	// 	// TODO: Implement congestion control logic
	// 	core.Log.Debug(s, "Congestion NACK on face", "face", inFace, "prefix", prefix)

	case spec.NackReasonNoRoute:
		// No route NACK - remove from active faces
		// Remove from active faces list
		for i, activeFace := range info.ActiveFaces {
			if activeFace == inFace {
				info.ActiveFaces = append(info.ActiveFaces[:i], info.ActiveFaces[i+1:]...)
				// Adjust index if needed
				if info.ActiveFaceIndex >= len(info.ActiveFaces) {
					info.ActiveFaceIndex = 0
				}
				break
			}
		}
		core.Log.Debug(s, "NoRoute NACK - removed from active faces", "face", inFace, "prefix", prefix)

	// case spec.NackReasonDuplicate:
	// 	// Duplicate NACK - usually safe to ignore, but log it
	// 	core.Log.Trace(s, "Duplicate NACK received", "face", inFace, "prefix", prefix)

	default:
		// Other NACK reasons
		core.Log.Debug(s, "Other NACK reason", "reason", nackReason, "face", inFace, "prefix", prefix)
	}

	// 3. Forward NACK downstream
	s.forwardNackDefault(packet, pitEntry, inFace, nackReason)
}

// THREAD SAFETY: Protected by info.mu
func (s *Multipath) handlePathDiscoveryData(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
	// 1. Lookup FIB to get Prefix
	prefix := "/"
	if len(packet.Name) > 0 {
		prefix = packet.Name[0].String()
	}

	info := s.getPathDiscoveryInfo(prefix)

	// Decode the full packet to access the internal Content.
	// This is stateless and safe.
	parsedData, _, err := spec.Spec{}.ReadData(enc.NewWireView(packet.Raw))
	if err != nil {
		core.Log.Error(s, "Failed to decode Data packet", "err", err, "name", packet.Name)
		// Forward anyway for robustness
		for faceID := range pitEntry.InRecords() {
			s.SendData(packet, pitEntry, faceID, inFace)
		}
		return
	}

	// Deserialize Metadata using ONLY the payload
	dp, err := datapacket.DeserializeDiscovery(parsedData.Content().Join())
	if err != nil {
		core.Log.Error(s, "Failed to deserialize DiscoveryPacket", "err", err)
		return
	}

	info.mu.Lock()
	defer info.mu.Unlock()

	core.Log.Debug(s, "Received PD Data", "name", packet.Name, "inFace", inFace, "TierConverged", dp.TierConverged, "NodeConverged", dp.NodeConverged)

	// 3. Process Convergence Logic
	if dp.TierConverged {
		if !info.IsPdFrozen {
			core.Log.Debug(s, "Removing converged upstream tier", "face", inFace, "prefix", prefix)
			delete(info.ConvergedUpstreamTier, inFace)
		} else {
			core.Log.Debug(s, "Frozen flag is set, ignore data for convergence", "face", inFace, "prefix", prefix)
		}

		if len(info.ConvergedUpstreamTier) == 0 {
			dp.TierConverged = true
			core.Log.Debug(s, "Tier converged", "tier", info.CurrentTier, "prefix", prefix)
			// Check if node converged
			if !info.IsNodeConverged && s.areHigherTiersEmpty(info) {
				s.localNodeConverged(info)
			}
		} else {
			dp.TierConverged = false
		}
	}

	// Check if this is a producer node (starts with "pro")
	nodeName := s.StrategyNodeName.String()
	if strings.HasPrefix(nodeName, "/pro") {
		dp.TierConverged = true
	}

	if dp.NodeConverged {
		core.Log.Debug(s, "Upstream node converged", "face", inFace, "prefix", prefix)
		delete(info.ConvergedUpstreamNode, inFace)

		if info.IsNodeConverged && len(info.ConvergedUpstreamNode) == 0 {
			dp.NodeConverged = true
			core.Log.Debug(s, "Node converged", "prefix", prefix)
		} else {
			dp.NodeConverged = false
		}
	} else {
		dp.NodeConverged = false
	}

	core.Log.Debug(s, "Sent new PD Data", "name", packet.Name, "inFace", inFace, "TierConverged", dp.TierConverged, "NodeConverged", dp.NodeConverged)

	// 4. Serialize updated metadata
	newContent, err := dp.SerializeDiscovery()
	if err != nil {
		core.Log.Error(s, "Failed to serialize DiscoveryPacket", "err", err)
		return
	}

	// Re-construct and Re-encode the Packet
	data, ok := parsedData.(*spec.Data)
	if !ok {
		core.Log.Error(s, "Failed to type assert Data packet")
		return
	}

	data.ContentV = enc.Wire{newContent}

	packetEncoder := spec.PacketEncoder{}
	packetEncoder.Init(&spec.Packet{Data: data})
	newWire := packetEncoder.Encode(&spec.Packet{Data: data})

	packet.Raw = newWire

	for faceID := range pitEntry.InRecords() {
		s.SendData(packet, pitEntry, faceID, inFace)
	}
}

// TODO: We need to implement the "dt" mode congestion control later
// handleTransmissionData processes "dt" (DT) packets
// THREAD SAFETY: Stateless (safe).
func (s *Multipath) handleTransmissionData(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
	// 1. Decode the full packet to access the internal Content
	parsedData, _, err := spec.Spec{}.ReadData(enc.NewWireView(packet.Raw))
	if err != nil {
		core.Log.Error(s, "Failed to decode Data packet", "err", err, "name", packet.Name)
		// Forward anyway for robustness
		for faceID := range pitEntry.InRecords() {
			s.SendData(packet, pitEntry, faceID, inFace)
		}
		return
	}

	// 2. Extract Metadata using DataPacket from datapacket.go
	originalContent := parsedData.Content().Join()
	dataPayloadSizeBytes := len(originalContent)
	if dataPayloadSizeBytes <= 0 {
		dataPayloadSizeBytes = 1
	}
	interestQsf := 0.0
	dataQsf := 0.0
	metadataDecoded := false
	dtPacket, err := datapacket.DeserializeData(originalContent)
	if err != nil {
		// Log warning but allow forwarding (robustness)
		core.Log.Warn(s, "Failed to deserialize DataPacket", "err", err, "name", packet.Name)
	} else {
		metadataDecoded = true
		interestQsf = dtPacket.InterestQsf
		dataQsf = dtPacket.DataQsf

		// Test serialization: Serialize the deserialized packet back to verify it works correctly
		serializedContent, err := dtPacket.SerializeData()
		if err != nil {
			core.Log.Warn(s, "Failed to serialize DataPacket", "err", err, "name", packet.Name)
		} else {
			// Verify that serialized content matches original content
			if len(serializedContent) != len(originalContent) {
				core.Log.Warn(s, "Serialization size mismatch",
					"original", len(originalContent),
					"serialized", len(serializedContent),
					"name", packet.Name)
			} else {
				// Compare serialized content with original content
				if !bytes.Equal(originalContent, serializedContent) {
					core.Log.Warn(s, "Serialization content mismatch", "name", packet.Name)
				} else {
					// Success - deserialization and serialization work correctly
					core.Log.Trace(s, "DataPacket serialization test passed", "name", packet.Name)
				}
			}
		}
	}

	// DT-only per-upstream measurement update (bandwidth + metadata in the same window).
	// Note: This tracks received metadata from upstream.
	prefix := "/"
	if len(packet.Name) > 0 {
		prefix = packet.Name[0].String()
	}
	info := s.getPathDiscoveryInfo(prefix)
	now := time.Now()
	info.mu.Lock()
	s.updateDtUpstreamMeasurements(info, prefix, inFace, now, len(originalContent), interestQsf, dataQsf)
	s.maybeInitDtControlPeriodFromRtt(info, pitEntry, inFace, now)
	interestQueueQsf := float64(len(info.TxInterestQueue))
	upstreamEstimatedBW := info.UpstreamEstimatedBW[inFace]
	upstreamInterestQsfAvg := info.UpstreamQsfInterest[inFace]
	upstreamDataQsfAvg := info.UpstreamQsfData[inFace]
	upstreamInterestQsfSlope := info.UpstreamQsfInterestSlope[inFace]
	upstreamDataQsfSlope := info.UpstreamQsfDataSlope[inFace]
	upstreamWindowSamples := len(info.UpstreamSamples[inFace])
	info.mu.Unlock()

	// Extract sequence number from data name and log metadata every 100 packets
	dataName := packet.Name.String()
	seqNum := extractSeqNum(dataName)

	if seqNum >= 0 && seqNum%100 == 0 {
		core.Log.Debug(s, "DT metadata received",
			"name", dataName,
			"seq", seqNum,
			"inFace", inFace,
			"metadataDecoded", metadataDecoded,
			"rxInterestQsf", interestQsf,
			"rxDataQsf", dataQsf,
			"windowAvgInterestQsf", upstreamInterestQsfAvg,
			"windowAvgDataQsf", upstreamDataQsfAvg,
			"windowSlopeInterestQsf", upstreamInterestQsfSlope,
			"windowSlopeDataQsf", upstreamDataQsfSlope,
			"estimatedBWPps", upstreamEstimatedBW,
			"windowSamples", upstreamWindowSamples)
	}
	if seqNum >= 0 && seqNum%200 == 0 {
		core.Log.Info(s, "Data Forwarded",
			"name", dataName,
			"seq", seqNum)
	}

	// DT rate-control rounds are triggered by the forwarding-thread periodic ticker.

	// 3. Forwarding Logic
	data, ok := parsedData.(*spec.Data)
	if !ok {
		core.Log.Warn(s, "Failed to type assert DT Data packet, fallback to unmodified forwarding", "name", packet.Name)
		for faceID := range pitEntry.InRecords() {
			s.SendData(packet, pitEntry, faceID, inFace)
		}
		return
	}

	for faceID := range pitEntry.InRecords() {
		// Update outgoing DT metadata with local queue signals:
		// - InterestQsf: local per-prefix DT Interest queue size
		// - DataQsf: downstream face transport send-queue size converted to packet units
		downstreamDataQsf := 0.0
		if outFace := dispatch.GetFace(faceID); outFace != nil {
			queueBytes := outFace.GetSendQueueSize()
			downstreamDataQsf = float64(queueBytes) / float64(dataPayloadSizeBytes)
		} else {
			core.Log.Warn(s, "Downstream face missing while updating DT metadata", "faceid", faceID, "name", packet.Name)
		}

		// If payload cannot be decoded, keep current forwarding behavior.
		if dtPacket == nil {
			s.SendData(packet, pitEntry, faceID, inFace)
			continue
		}

		faceDtPacket := *dtPacket
		faceDtPacket.InterestQsf = interestQueueQsf
		faceDtPacket.DataQsf = downstreamDataQsf

		newContent, serializeErr := faceDtPacket.SerializeData()
		if serializeErr != nil {
			core.Log.Warn(s, "Failed to serialize DT metadata update, fallback to unmodified forwarding",
				"err", serializeErr, "faceid", faceID, "name", packet.Name)
			s.SendData(packet, pitEntry, faceID, inFace)
			continue
		}

		faceData := *data
		faceData.ContentV = enc.Wire{newContent}

		packetEncoder := spec.PacketEncoder{}
		packetEncoder.Init(&spec.Packet{Data: &faceData})
		newWire := packetEncoder.Encode(&spec.Packet{Data: &faceData})

		packetForFace := *packet
		packetForFace.Raw = newWire
		s.SendData(&packetForFace, pitEntry, faceID, inFace)
	}
}

func getDtFaceInterestSignalState(face uint64) *dtFaceInterestSignalState {
	val, _ := dtFaceInterestSignalByFace.LoadOrStore(face, &dtFaceInterestSignalState{
		byPref: make(map[string]dtPrefixInterestSignal),
	})
	return val.(*dtFaceInterestSignalState)
}

func getDtFaceControlState(face uint64) *dtFaceControlState {
	val, _ := dtFaceControlByFace.LoadOrStore(face, &dtFaceControlState{})
	return val.(*dtFaceControlState)
}

// updateDtFaceInterestSignal keeps per-prefix signal snapshots for one upstream face.
// It reuses existing sliding-window outputs and adds no packet-level metadata overhead.
func (s *Multipath) updateDtFaceInterestSignal(
	face uint64,
	prefix string,
	interestQ float64,
	interestSlope float64,
	dataQ float64,
	dataSlope float64,
	bwPps float64,
	sampleCount int,
	sampleTime time.Time,
) {
	state := getDtFaceInterestSignalState(face)
	state.mu.Lock()
	state.byPref[prefix] = dtPrefixInterestSignal{
		InterestQ:     interestQ,
		InterestSlope: interestSlope,
		DataQ:         dataQ,
		DataSlope:     dataSlope,
		BWPps:         bwPps,
		SampleCount:   sampleCount,
		LastUpdated:   sampleTime,
	}
	state.mu.Unlock()
}

// getAveragedDtFaceSignal returns face-level signals across prefixes for one upstream face.
// Queue-related signals are weighted averages across prefixes.
// Bandwidth signal is total pps summed across live prefixes (not averaged).
func (s *Multipath) getAveragedDtFaceSignal(face uint64) (float64, float64, float64, float64, float64, int) {
	state := getDtFaceInterestSignalState(face)
	state.mu.Lock()
	defer state.mu.Unlock()

	if len(state.byPref) == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	now := time.Now()
	liveCutoff := now.Add(-dtThroughputWindowDur)

	var sumW float64
	var sumIQ float64
	var sumIQSlope float64
	var sumDQ float64
	var sumDQSlope float64
	var sumBWTotal float64
	var countedPrefixes int
	for prefix, signal := range state.byPref {
		// Prefix is live only if it still has a recent sample inside window.
		if signal.LastUpdated.Before(liveCutoff) {
			delete(state.byPref, prefix)
			continue
		}
		w := float64(signal.SampleCount)
		if w <= 0 {
			continue
		}
		sumW += w
		sumIQ += w * signal.InterestQ
		sumIQSlope += w * signal.InterestSlope
		sumDQ += w * signal.DataQ
		sumDQSlope += w * signal.DataSlope
		sumBWTotal += signal.BWPps
		countedPrefixes++
	}

	if sumW <= 0 {
		return 0, 0, 0, 0, 0, 0
	}

	return sumIQ / sumW, sumIQSlope / sumW, sumDQ / sumW, sumDQSlope / sumW, sumBWTotal, countedPrefixes
}

func clampDtControlPeriod(p time.Duration) time.Duration {
	if p < dtControlPeriodMin {
		return dtControlPeriodMin
	}
	if p > dtControlPeriodMax {
		return dtControlPeriodMax
	}
	return p
}

func getDtRttBootstrapState(face uint64) *dtRttBootstrapState {
	val, _ := dtRttBootstrapByFace.LoadOrStore(face, &dtRttBootstrapState{})
	return val.(*dtRttBootstrapState)
}

func getDtRttBootstrapPeriod(face uint64) (bool, time.Duration, int) {
	state := getDtRttBootstrapState(face)
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.ready, state.period, state.count
}

func recordDtRttBootstrapSample(face uint64, rtt time.Duration) (bool, time.Duration, int) {
	if rtt <= 0 {
		return false, 0, 0
	}

	state := getDtRttBootstrapState(face)
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.ready {
		return true, state.period, state.count
	}

	state.count++
	state.sum += rtt
	if state.count < dtControlRttInitSamples {
		return false, 0, state.count
	}

	avg := state.sum / time.Duration(state.count)
	scaled := time.Duration(float64(avg) * dtControlPeriodRttScale)
	state.period = clampDtControlPeriod(scaled)
	state.ready = true
	return true, state.period, state.count
}

// ensureDtControlActivated marks candidate faces and starts per-face scheduler ownership.
// It does not initialize per-face control period until RTT bootstrap is ready.
// THREAD SAFETY: caller must hold info.mu.
func (s *Multipath) ensureDtControlActivated(
	prefix string,
	candidateFaces []uint64,
	now time.Time,
) {
	for _, face := range candidateFaces {
		control := getDtFaceControlState(face)
		control.mu.Lock()
		if !control.activated {
			control.activated = true
			core.Log.Info(s, "DT periodic control activated", "prefix", prefix, "face", face)
		}

		if control.period <= 0 {
			ready, period, samples := getDtRttBootstrapPeriod(face)
			if ready {
				control.period = period
				control.nextRun = now
				core.Log.Info(s, "DT control period ready from bootstrap history",
					"prefix", prefix, "face", face, "period", period, "rttSamples", samples,
					"periodScale", dtControlPeriodRttScale)
			}
		}
		control.mu.Unlock()
	}
}

// maybeInitDtControlPeriodFromRtt records RTT sample and initializes per-face control period
// when RTT bootstrap reaches required sample count.
// THREAD SAFETY: caller must hold info.mu.
func (s *Multipath) maybeInitDtControlPeriodFromRtt(
	info *PathDiscoveryInfo,
	pitEntry table.PitEntry,
	upstreamFace uint64,
	now time.Time,
) {
	control := getDtFaceControlState(upstreamFace)
	control.mu.Lock()
	if !control.activated {
		control.mu.Unlock()
		return
	}
	if control.period > 0 {
		control.mu.Unlock()
		return
	}

	outRecord, ok := pitEntry.OutRecords()[upstreamFace]
	if !ok || outRecord == nil {
		control.mu.Unlock()
		return
	}
	rtt := now.Sub(outRecord.LatestTimestamp)
	ready, period, samples := recordDtRttBootstrapSample(upstreamFace, rtt)
	if !ready {
		control.mu.Unlock()
		return
	}

	control.period = period
	control.nextRun = now
	control.mu.Unlock()

	core.Log.Info(s, "DT control period initialized",
		"prefix", info.Prefix, "face", upstreamFace, "period", period, "rttSamples", samples,
		"periodScale", dtControlPeriodRttScale)
}

// OnTick runs periodic DT per-face rate control.
// This keeps control scheduling independent from Interest/Data/Nack packet arrivals.
func (s *Multipath) OnTick(now time.Time) {
	dtFaceControlByFace.Range(func(key, value any) bool {
		face, ok := key.(uint64)
		if !ok {
			return true
		}

		control, ok := value.(*dtFaceControlState)
		if !ok || control == nil {
			return true
		}

		shouldRun := false
		control.mu.Lock()
		if control.activated && control.period > 0 {
			if control.nextRun.IsZero() || !now.Before(control.nextRun) {
				control.nextRun = now.Add(control.period)
				shouldRun = true
			}
		}
		control.mu.Unlock()

		if shouldRun {
			s.faceSendRateEstimationCore(face)
		}
		return true
	})
}

func initDtEstimatorTunables() {
	dtEstimatorTunableOnce.Do(func() {
		currentPktSize := datapacket.NewDataPacket().GetSize()
		if currentPktSize > 0 {
			dtDataPacketSizeBytes = currentPktSize
		}

		// Keep DT sliding-window duration static (no chunk-size shaping).
		dtThroughputWindowDur = dtBaseThroughputWindow
		if dtThroughputWindowDur < dtMinThroughputWindow {
			dtThroughputWindowDur = dtMinThroughputWindow
		}

		dtMaxSamplesPerFace = int(dtMaxExpectedPps * dtThroughputWindowDur.Seconds() * dtWindowSafetyFactor)
		if dtMaxSamplesPerFace < 1 {
			dtMaxSamplesPerFace = 1
		}
	})
}

// updateDtUpstreamMeasurements maintains DT per-upstream sliding-window measurements.
// It updates:
//  1. bandwidth estimate in packets/sec and
//  2. average InterestQsf/DataQsf metadata
//  3. InterestQsf/DataQsf trend slopes (units: qsf per second), computed via
//     exponentially weighted linear regression (newer samples weighted more)
//
// using the same static window duration.
// THREAD SAFETY: caller must hold info.mu.
func (s *Multipath) updateDtUpstreamMeasurements(
	info *PathDiscoveryInfo,
	prefix string,
	upstreamFace uint64,
	now time.Time,
	payloadSize int,
	interestQsf float64,
	dataQsf float64,
) {
	samples := info.UpstreamSamples[upstreamFace]
	samples = append(samples, DtUpstreamSample{
		Timestamp:   now,
		SizeBytes:   payloadSize,
		InterestQsf: interestQsf,
		DataQsf:     dataQsf,
	})

	cutoff := now.Add(-dtThroughputWindowDur)
	validIdx := len(samples)
	for i, sample := range samples {
		if !sample.Timestamp.Before(cutoff) {
			validIdx = i
			break
		}
	}
	samples = samples[validIdx:]
	if len(samples) > dtMaxSamplesPerFace {
		samples = samples[len(samples)-dtMaxSamplesPerFace:]
	}
	info.UpstreamSamples[upstreamFace] = samples

	if len(samples) == 0 {
		info.UpstreamEstimatedBW[upstreamFace] = 0
		info.UpstreamQsfInterest[upstreamFace] = 0
		info.UpstreamQsfData[upstreamFace] = 0
		info.UpstreamQsfInterestSlope[upstreamFace] = 0
		info.UpstreamQsfDataSlope[upstreamFace] = 0
		s.updateDtFaceInterestSignal(upstreamFace, prefix, 0, 0, 0, 0, 0, 0, now)
		return
	}

	measuredPPS := float64(len(samples)) / dtThroughputWindowDur.Seconds()
	info.UpstreamEstimatedBW[upstreamFace] = measuredPPS

	var sumInterestQsf float64
	var sumDataQsf float64
	tauSec := dtThroughputWindowDur.Seconds() * dtSlopeTauRatio
	if tauSec <= 0 {
		tauSec = 1e-6
	}

	var sumW float64
	var sumWX float64
	var sumWXX float64
	var sumWI float64
	var sumWD float64
	var sumWXI float64
	var sumWXD float64
	for _, sample := range samples {
		sumInterestQsf += sample.InterestQsf
		sumDataQsf += sample.DataQsf

		x := sample.Timestamp.Sub(now).Seconds()
		if x > 0 {
			x = 0
		}
		w := math.Exp(x / tauSec)

		sumW += w
		sumWX += w * x
		sumWXX += w * x * x
		sumWI += w * sample.InterestQsf
		sumWD += w * sample.DataQsf
		sumWXI += w * x * sample.InterestQsf
		sumWXD += w * x * sample.DataQsf
	}
	info.UpstreamQsfInterest[upstreamFace] = sumInterestQsf / float64(len(samples))
	info.UpstreamQsfData[upstreamFace] = sumDataQsf / float64(len(samples))

	interestSlope := 0.0
	dataSlope := 0.0
	if len(samples) >= 2 {
		den := (sumW * sumWXX) - (sumWX * sumWX)
		if math.Abs(den) > 1e-12 {
			interestSlope = ((sumW * sumWXI) - (sumWX * sumWI)) / den
			dataSlope = ((sumW * sumWXD) - (sumWX * sumWD)) / den
		}
	}
	info.UpstreamQsfInterestSlope[upstreamFace] = interestSlope
	info.UpstreamQsfDataSlope[upstreamFace] = dataSlope
	s.updateDtFaceInterestSignal(
		upstreamFace,
		prefix,
		info.UpstreamQsfInterest[upstreamFace],
		interestSlope,
		info.UpstreamQsfData[upstreamFace],
		dataSlope,
		info.UpstreamEstimatedBW[upstreamFace],
		len(samples),
		now,
	)
}

// faceSendRateEstimationCore performs one per-upstream-face control round.
// All input signals and control state are aggregated/stored at face level.
func (s *Multipath) faceSendRateEstimationCore(upstreamFace uint64) {
	// Step 1: derive queue signal from face-level aggregates across prefixes.
	interestQ, interestSlope, dataQ, dataSlope, estimatedBWFaceTotal, prefixes := s.getAveragedDtFaceSignal(upstreamFace)
	if prefixes <= 0 {
		return
	}

	// Default dominant signal is data. Switch to interest only when interest is strictly larger.
	currentQ := dataQ
	queueSlope := dataSlope
	selectedSignal := "data"
	if interestQ > dataQ {
		currentQ = interestQ
		queueSlope = interestSlope
		selectedSignal = "interest"
	}

	control := getDtFaceControlState(upstreamFace)
	control.mu.Lock()
	prevRate := control.rate
	currentRate := prevRate
	if currentRate <= 0 {
		currentRate = dtRateControlParams.FwdMinRate
	}

	// Step 3: Define thresholds.
	qthHigh := dtRateControlParams.MaxFwdQueueSize * dtRateControlParams.FwdQueueBeta
	qthLow := dtRateControlParams.MaxFwdQueueSize * dtRateControlParams.FwdQueueAlpha
	slopeThreshold := dtRateControlParams.FwdSlopeThreshold

	// Step 4: State-machine controller.
	newRate := currentRate
	primaryAction := DtActionHoldDraining

	if currentQ >= qthHigh {
		if queueSlope > slopeThreshold {
			newRate = currentRate * dtRateControlParams.FwdMDFactor
			primaryAction = DtActionEmergencyDecrease
		} else if queueSlope < -slopeThreshold {
			newRate = currentRate
			primaryAction = DtActionHoldDraining
		} else {
			newRate = currentRate * dtRateControlParams.FwdGDFactor
			primaryAction = DtActionGentleDecrease
		}
	} else if currentQ >= qthLow {
		newRate = currentRate
		primaryAction = DtActionHoldDraining
	} else {
		if queueSlope > slopeThreshold {
			if estimatedBWFaceTotal > 0 {
				newRate = estimatedBWFaceTotal * dtRateControlParams.FwdCIFactor
			} else {
				newRate = currentRate * dtRateControlParams.FwdCIFactor
			}
			primaryAction = DtActionCautiousIncrease
		} else {
			if estimatedBWFaceTotal > 0 {
				newRate = estimatedBWFaceTotal * dtRateControlParams.FwdRPFactor
			} else {
				newRate = currentRate * dtRateControlParams.FwdRPFactor
			}
			primaryAction = DtActionAggressiveProbe
		}
	}

	// Step 5: Bandwidth safety guard.
	finalAction := primaryAction
	if estimatedBWFaceTotal > 0 {
		maxSafeRate := estimatedBWFaceTotal * dtRateControlParams.MaxBWSafetyRatio
		if newRate > maxSafeRate {
			newRate = maxSafeRate
			finalAction = DtActionBWCapped
		}

		minSafeRate := estimatedBWFaceTotal * dtRateControlParams.MinBWSafetyRatio
		if newRate < minSafeRate {
			newRate = minSafeRate
			finalAction = DtActionBWBoosted
		}
	}

	// Step 6: Global safety bounds.
	if newRate < dtRateControlParams.FwdMinRate {
		newRate = dtRateControlParams.FwdMinRate
		finalAction = DtActionGlobalMinCapped
	}
	if dtRateControlParams.FwdMaxRate > 0 && newRate > dtRateControlParams.FwdMaxRate {
		newRate = dtRateControlParams.FwdMaxRate
		finalAction = DtActionGlobalMaxCapped
	}

	// Step 7: persist and emit concise per-round telemetry.
	control.rate = newRate
	control.action = finalAction
	control.mu.Unlock()

	bwMbpsFaceTotal := 0.0
	if estimatedBWFaceTotal > 0 {
		packetBytes := dtDataPacketSizeBytes
		if packetBytes <= 0 {
			packetBytes = datapacket.NewDataPacket().GetSize()
		}
		if packetBytes > 0 {
			bwMbpsFaceTotal = (estimatedBWFaceTotal * float64(packetBytes) * 8.0) / 1e6
		}
	}

	core.Log.Debug(s, "DT rate-control round",
		"face", upstreamFace,
		"prefixes", prefixes,
		"signal", selectedSignal,
		"interestQFaceAvg", interestQ,
		"interestSlopeFaceAvg", interestSlope,
		"dataQFaceAvg", dataQ,
		"dataSlopeFaceAvg", dataSlope,
		"q", currentQ,
		"slope", queueSlope,
		"bwPpsFaceTotal", estimatedBWFaceTotal,
		"bwMbpsFaceTotal", bwMbpsFaceTotal,
		"ratePrev", prevRate,
		"rateNew", newRate,
		"action", dtRateActionString(finalAction))
}

// ---- Helper Functions ----

// areHigherTiersEmpty checks if tiers > currentTier are empty
func (s *Multipath) areHigherTiersEmpty(info *PathDiscoveryInfo) bool {
	for i := info.CurrentTier + 1; i < len(info.WaitList); i++ {
		if len(info.WaitList[i]) > 0 {
			return false
		}
	}
	return true
}

// isCurrentTierAllInvalid checks if all faces in the current tier are invalid
func (s *Multipath) isCurrentTierAllInvalid(info *PathDiscoveryInfo) bool {
	if info.CurrentTier >= len(info.TierList) {
		return true
	}

	// Check if any face in the current tier (from static TierList) is still in ValidUpstreams
	currentTierFaces := info.TierList[info.CurrentTier]
	for _, hop := range currentTierFaces {
		if info.ValidUpstreams[hop.Nexthop] {
			return false // Found a valid one
		}
	}
	return true // All invalid
}

// skipTier clears remaining waitlists and forces convergence
func (s *Multipath) skipTier(info *PathDiscoveryInfo) {
	// Erase all faces from upper tiers of waitlist (currentTier and above)
	// Also remove them from ValidUpstreams and ConvergedUpstreamNode
	for i := info.CurrentTier; i < len(info.WaitList); i++ {
		facesToRemove := info.WaitList[i]

		// Clear the tier in WaitList
		info.WaitList[i] = []hopSnapshot{}

		for _, hop := range facesToRemove {
			// Remove from ValidUpstreams
			delete(info.ValidUpstreams, hop.Nexthop)
			// Remove from ConvergedUpstreamNode
			delete(info.ConvergedUpstreamNode, hop.Nexthop)
		}
	}

	// Set node as converged
	core.Log.Info(s, "Skipped tier triggered", "prefix", info.Prefix, "tier", info.CurrentTier)
	if !info.IsNodeConverged {
		s.localNodeConverged(info)
	}

}

// localNodeConverged handles the logic when the local node converges
// THREAD SAFETY: Must be called with info.mu.Lock() held by the caller
// With shared multipathPdState across all threads, this ensures only one thread
// can trigger convergence for a given prefix, preventing duplicate convergence calls
func (s *Multipath) localNodeConverged(info *PathDiscoveryInfo) {
	// Double-check pattern: even though caller should have lock, this prevents
	// duplicate work if somehow called multiple times (defensive programming)
	if !info.IsNodeConverged {
		// Clear converged upstream tier set
		info.ConvergedUpstreamTier = make(map[uint64]bool)
		info.IsNodeConverged = true

		// Update Tier List based on Wait List (Finalizing Valid Upstreams)
		s.updateAfterPathDiscovery(info)

		// --- LOGGING THE RESULTS FOR DT PHASE ---
		core.Log.Info(s, "PD Converged: Resulting Nexthops for DT", "prefix", info.Prefix)
		for i, tier := range info.TierList {
			var faces []uint64
			for _, nh := range tier {
				faces = append(faces, nh.Nexthop)
			}
			// Log the tier index, the cost associated with this tier, and the list of valid faces
			cost := uint64(0)
			if i < len(info.TierCostList) {
				cost = info.TierCostList[i]
			}
			core.Log.Info(s, " - Tier", "index", i, "cost", cost, "faces", faces)
		}
		// ----------------------------------------

		// TODO: Activate Data Transmission Scheduler
		// TODO: Implement dt initialization logic later
		// s.activateDTScheduler(info)

		core.Log.Info(s, "Local Node Converged", "prefix", info.Prefix)
	}
}

// updateAfterPathDiscovery rebuilds the TierList based on valid results
func (s *Multipath) updateAfterPathDiscovery(info *PathDiscoveryInfo) {
	var newTierList [][]hopSnapshot
	var newCostList []uint64

	for i, tier := range info.WaitList {
		if len(tier) > 0 {
			newTier := make([]hopSnapshot, len(tier))
			copy(newTier, tier)
			newTierList = append(newTierList, newTier)
			if i < len(info.TierCostList) {
				newCostList = append(newCostList, info.TierCostList[i])
			}
		}
	}

	info.TierList = newTierList
	info.TierCostList = newCostList
}

// Helper to remove face from WaitList
func (s *Multipath) removeFaceFromWaitList(info *PathDiscoveryInfo, face uint64) {
	for i := range info.WaitList {
		var newTier []hopSnapshot
		for _, hop := range info.WaitList[i] {
			if hop.Nexthop != face {
				newTier = append(newTier, hop)
			}
		}
		info.WaitList[i] = newTier
	}
}

// Helper to remove face from ProbedUpstreams
func (s *Multipath) removeFaceFromProbedUpstreams(info *PathDiscoveryInfo, face uint64) {
	for i, f := range info.ProbedUpstreams {
		if f == face {
			info.ProbedUpstreams = append(info.ProbedUpstreams[:i], info.ProbedUpstreams[i+1:]...)
			break
		}
	}
}
