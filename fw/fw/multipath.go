package fw

import (
	"bytes"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/named-data/ndnd/fw/core"
	"github.com/named-data/ndnd/fw/defn"
	"github.com/named-data/ndnd/fw/table"
	enc "github.com/named-data/ndnd/std/encoding"
	spec "github.com/named-data/ndnd/std/ndn/spec_2022"
)

// PdQueueItem represents an item in the Interest Queue (m_pdInterestQueue).
type PdQueueItem struct {
	Packet   *defn.Pkt
	PitEntry table.PitEntry
	InFace   uint64
}

// PathDiscoveryInfo holds all state for a specific prefix (Namespace).
type PathDiscoveryInfo struct {
	mu sync.Mutex

	Prefix        string
	IsInitialized bool

	// ---- Hop-tier grouping ----
	TierList     [][]*table.FibNextHopEntry
	TierCostList []uint64
	CurrentTier  int

	// ---- Path Discovery State ----
	ProbeIndex           int
	WaitList             [][]*table.FibNextHopEntry
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
	InterestQueue []*PdQueueItem

	// ---- Scheduler ----
	Ticker     *time.Ticker
	StopTicker chan struct{}

	// ---- Data Transmission ----
	ActiveFaces     []uint64
	ActiveFaceIndex int

	// ---- Round-Robin Counter (for simple forwarding) (ATOMIC) ----
	RoundRobinCounter uint64
}

// Shared state across all Multipath strategy instances (all threads)
// This ensures that PathDiscoveryInfo is shared across threads, not duplicated per thread
var multipathPdState sync.Map

// Multipath Strategy Struct
type Multipath struct {
	StrategyBase
}

func init() {
	strategyInit = append(strategyInit, func() Strategy { return &Multipath{} })
	StrategyVersions["multipath"] = []uint64{1}
}

func (s *Multipath) Instantiate(fwThread *Thread) {
	s.NewStrategyBase(fwThread, "multipath", 1)
}

// Helper to get or create the state for a prefix
// THREAD SAFETY: Uses package-level multipathPdState shared across all threads
func (s *Multipath) getPathDiscoveryInfo(prefix string) *PathDiscoveryInfo {
	val, loaded := multipathPdState.LoadOrStore(prefix, &PathDiscoveryInfo{
		Prefix:                prefix,
		ConvergedUpstreamTier: make(map[uint64]bool),
		ConvergedUpstreamNode: make(map[uint64]bool),
		ValidUpstreams:        make(map[uint64]bool),
		FrozenFaces:           make(map[uint64]bool),
		FrozenRecoverFaces:    make(map[uint64]bool),
		InterestQueue:         make([]*PdQueueItem, 0),
		StopTicker:            make(chan struct{}),
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

	// 2. Identify the Flow/Prefix
	var prefixKey string
	if len(packet.Name) > 0 {
		prefixKey = packet.Name[0].String()
	} else {
		prefixKey = "/"
	}

	// 3. Get Info
	info := s.getPathDiscoveryInfo(prefixKey)

	// 4. Atomic Round-Robin Selection
	// THREAD SAFETY: Atomic increment is safe without mutex.
	currentCount := atomic.AddUint64(&info.RoundRobinCounter, 1) - 1
	selectedIndex := currentCount % uint64(len(sortedHops))
	targetFace := sortedHops[selectedIndex].Nexthop

	// 5. Send Immediately
	s.SendInterest(packet, pitEntry, targetFace, inFace)
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

		s.TierGrouping(info, nexthops)
		s.StartPathDiscovery(info)
	}

	// 3. Tier Probing Logic
	if !info.IsNodeConverged {
		if probeIdx > info.ProbeIndex {
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

func (s *Multipath) TierGrouping(info *PathDiscoveryInfo, nexthops []*table.FibNextHopEntry) {
	// Group NextHops into tiers according to hop count (cost)
	costToFaces := make(map[uint64][]*table.FibNextHopEntry)
	var costs []uint64

	for _, nh := range nexthops {
		if _, exists := costToFaces[nh.Cost]; !exists {
			costs = append(costs, nh.Cost)
		}
		costToFaces[nh.Cost] = append(costToFaces[nh.Cost], nh)
	}

	// Sort costs to ensure tiers are ordered
	sort.Slice(costs, func(i, j int) bool { return costs[i] < costs[j] })

	// Build TierList
	info.TierList = make([][]*table.FibNextHopEntry, 0)
	info.TierCostList = make([]uint64, 0)

	for _, cost := range costs {
		info.TierList = append(info.TierList, costToFaces[cost])
		info.TierCostList = append(info.TierCostList, cost)
		core.Log.Debug(s, "TierGrouping", "cost", cost, "facesCount", len(costToFaces[cost]))
	}

	// Initialize WaitList as a copy of TierList
	info.WaitList = make([][]*table.FibNextHopEntry, len(info.TierList))
	copy(info.WaitList, info.TierList)

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
				info.WaitList[i] = []*table.FibNextHopEntry{}
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

	// ! Send NACK with "NONE" reason
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
	dp, err := DeserializeDiscovery(parsedData.Content().Join())
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
	dtPacket, err := DeserializeData(originalContent)
	if err != nil {
		// Log warning but allow forwarding (robustness)
		core.Log.Warn(s, "Failed to deserialize DataPacket", "err", err, "name", packet.Name)
	} else {
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
		// TODO: Implement Sliding Window (SW) logic here if needed using dtPacket.InterestQsf / dtPacket.DataQsf
	}

	// Extract sequence number from data name and log every 200 packets
	dataName := packet.Name.String()
	seqNum := extractSeqNum(dataName)
	if seqNum >= 0 && seqNum%200 == 0 {
		core.Log.Info(s, "Data Forwarded",
			"name", dataName,
			"seq", seqNum)
	}

	// 2. Bandwidth Estimation & Congestion Control (Placeholder)
	// s.FaceBandwidthEstimation(inFace)
	// s.FaceSendRateEstimation(inFace)

	// 3. Forwarding Logic
	for faceID := range pitEntry.InRecords() {
		// If we needed to update QSF values per downstream:
		// 1. Modify dtPacket.InterestQsf / dtPacket.DataQsf
		// 2. newContent, _ := dtPacket.SerializeData()
		// 3. packet.Content = newContent
		s.SendData(packet, pitEntry, faceID, inFace)
	}
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
		info.WaitList[i] = []*table.FibNextHopEntry{}

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

// // localNodeConverged handles the logic when the local node converges
// // THREAD SAFETY: Must be called with info.mu.Lock() held by the caller
// // With shared multipathPdState across all threads, this ensures only one thread
// // can trigger convergence for a given prefix, preventing duplicate convergence calls
// func (s *Multipath) localNodeConverged(info *PathDiscoveryInfo) {
// 	// Double-check pattern: even though caller should have lock, this prevents
// 	// duplicate work if somehow called multiple times (defensive programming)
// 	if !info.IsNodeConverged {
// 		// Clear converged upstream tier set
// 		info.ConvergedUpstreamTier = make(map[uint64]bool)
// 		info.IsNodeConverged = true

// 		// Update Tier List based on Wait List (Finalizing Valid Upstreams)
// 		s.updateAfterPathDiscovery(info)

// 		// TODO: Activate Data Transmission Scheduler
// 		// TODO: Implement dt initialization logic later
// 		// s.activateDTScheduler(info)

// 		core.Log.Info(s, "Local Node Converged", "prefix", info.Prefix)
// 	}
// }

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
	var newTierList [][]*table.FibNextHopEntry
	var newCostList []uint64

	for i, tier := range info.WaitList {
		if len(tier) > 0 {
			newTierList = append(newTierList, tier)
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
		var newTier []*table.FibNextHopEntry
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

// package fw

// import (
// 	"bytes"
// 	"sort"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"sync/atomic"
// 	"time"

// 	"github.com/named-data/ndnd/fw/core"
// 	"github.com/named-data/ndnd/fw/defn"
// 	"github.com/named-data/ndnd/fw/table"
// 	enc "github.com/named-data/ndnd/std/encoding"
// 	spec "github.com/named-data/ndnd/std/ndn/spec_2022"
// )

// // PdQueueItem represents an item in the Interest Queue (m_pdInterestQueue).
// type PdQueueItem struct {
// 	Packet   *defn.Pkt
// 	PitEntry table.PitEntry
// 	InFace   uint64
// }

// // PathDiscoveryInfo holds all state for a specific prefix (Namespace).
// type PathDiscoveryInfo struct {
// 	mu sync.Mutex

// 	Prefix        string
// 	IsInitialized bool

// 	// ---- Hop-tier grouping ----
// 	TierList     [][]*table.FibNextHopEntry
// 	TierCostList []uint64
// 	CurrentTier  int

// 	// ---- Path Discovery State ----
// 	ProbeIndex           int
// 	WaitList             [][]*table.FibNextHopEntry
// 	ProbedUpstreams      []uint64
// 	ProbedSchedulerIndex int

// 	// ---- Convergence State ----
// 	ConvergedUpstreamTier map[uint64]bool
// 	ConvergedUpstreamNode map[uint64]bool
// 	IsNodeConverged       bool
// 	ValidUpstreams        map[uint64]bool
// 	IsInvalidNode         bool
// 	PrefixPdSendRate      float64

// 	// ---- Frozen / Race Condition Handling ----
// 	IsPdFrozen         bool
// 	FrozenFaces        map[uint64]bool
// 	FrozenRecoverFaces map[uint64]bool

// 	// ---- Queue Management ----
// 	InterestQueue []*PdQueueItem

// 	// ---- Scheduler ----
// 	Ticker     *time.Ticker
// 	StopTicker chan struct{}

// 	// ---- Data Transmission ----
// 	ActiveFaces     []uint64
// 	ActiveFaceIndex int

// 	// ---- Round-Robin Counter (for simple forwarding) (ATOMIC) ----
// 	RoundRobinCounter uint64
// }

// // Shared state across all Multipath strategy instances (all threads)
// // This ensures that PathDiscoveryInfo is shared across threads, not duplicated per thread
// var multipathPdState sync.Map

// // Multipath Strategy Struct
// type Multipath struct {
// 	StrategyBase
// }

// func init() {
// 	strategyInit = append(strategyInit, func() Strategy { return &Multipath{} })
// 	StrategyVersions["multipath"] = []uint64{1}
// }

// func (s *Multipath) Instantiate(fwThread *Thread) {
// 	s.NewStrategyBase(fwThread, "multipath", 1)
// }

// // Helper to get or create the state for a prefix
// // THREAD SAFETY: Uses package-level multipathPdState shared across all threads
// func (s *Multipath) getPathDiscoveryInfo(prefix string) *PathDiscoveryInfo {
// 	val, loaded := multipathPdState.LoadOrStore(prefix, &PathDiscoveryInfo{
// 		Prefix:                prefix,
// 		ConvergedUpstreamTier: make(map[uint64]bool),
// 		ConvergedUpstreamNode: make(map[uint64]bool),
// 		ValidUpstreams:        make(map[uint64]bool),
// 		FrozenFaces:           make(map[uint64]bool),
// 		FrozenRecoverFaces:    make(map[uint64]bool),
// 		InterestQueue:         make([]*PdQueueItem, 0),
// 		StopTicker:            make(chan struct{}),
// 	})

// 	info := val.(*PathDiscoveryInfo)

// 	if !loaded {
// 		core.Log.Info(s, "Created new PathDiscoveryInfo for prefix", prefix)
// 	}

// 	return info
// }

// func (s *Multipath) AfterContentStoreHit(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
// 	s.SendData(packet, pitEntry, inFace, 0)
// }

// // AfterReceiveData implements the Data Pipeline logic
// func (s *Multipath) AfterReceiveData(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
// 	// 1. Identify Data Type ("pd" or "dt")
// 	// Assumption: Name format is /<prefix>/<node>/<type>/...
// 	if len(packet.Name) < 3 {
// 		core.Log.Warn(s, "Received Data with short name", "name", packet.Name)
// 		return
// 	}

// 	dataType := packet.Name[2].String()

// 	switch dataType {
// 	case "pd":
// 		s.handlePathDiscoveryData(packet, pitEntry, inFace)
// 	case "dt":
// 		s.handleTransmissionData(packet, pitEntry, inFace)
// 	default:
// 		core.Log.Warn(s, "Unknown Data type", "type", dataType, "name", packet.Name)
// 	}
// }

// // AfterReceiveInterest implements the Interest Pipeline logic
// func (s *Multipath) AfterReceiveInterest(
// 	packet *defn.Pkt,
// 	pitEntry table.PitEntry,
// 	inFace uint64,
// 	nexthops []*table.FibNextHopEntry,
// ) {
// 	// 1. Identify Interest Type
// 	// Assumption: Name format is /<prefix>/<node>/<type>/.../<probeIdx>/<seq>
// 	// C++: interest.getName().get(2) is type.
// 	if len(packet.Name) < 3 {
// 		core.Log.Warn(s, "Received Interest with short name", "name", packet.Name)
// 		return
// 	}

// 	interestType := packet.Name[2].String()

// 	if interestType == "pd" {
// 		s.handlePathDiscoveryInterest(packet, pitEntry, inFace, nexthops)
// 	} else {
// 		// Keep existing DT / default logic (Round Robin on nexthops)
// 		s.handleDataTransmissionInterest(packet, pitEntry, inFace, nexthops)
// 	}
// }

// // handleDataTransmissionInterest keeps the previous logic for non-pd packets
// func (s *Multipath) handleDataTransmissionInterest(
// 	packet *defn.Pkt,
// 	pitEntry table.PitEntry,
// 	inFace uint64,
// 	nexthops []*table.FibNextHopEntry,
// ) {
// 	// Extract sequence number from interest name and log every 200 packets
// 	interestName := packet.Name.String()
// 	seqNum := extractSeqNum(interestName)
// 	if seqNum >= 0 && seqNum%200 == 0 {
// 		core.Log.Info(s, "Interest Forwarded", "name", interestName, "seq", seqNum)
// 	}

// 	if len(nexthops) == 0 {
// 		core.Log.Warn(s, "No nexthops available for", packet.Name)
// 		return
// 	}

// 	// 1. Sort Nexthops
// 	sortedHops := make([]*table.FibNextHopEntry, len(nexthops))
// 	copy(sortedHops, nexthops)
// 	sort.Slice(sortedHops, func(i, j int) bool {
// 		return sortedHops[i].Nexthop < sortedHops[j].Nexthop
// 	})

// 	// 2. Identify the Flow/Prefix
// 	var prefixKey string
// 	if len(packet.Name) > 0 {
// 		prefixKey = packet.Name[0].String()
// 	} else {
// 		prefixKey = "/"
// 	}

// 	// 3. Get Info
// 	info := s.getPathDiscoveryInfo(prefixKey)

// 	// 4. Atomic Round-Robin Selection
// 	// THREAD SAFETY: Atomic increment is safe without mutex.
// 	currentCount := atomic.AddUint64(&info.RoundRobinCounter, 1) - 1
// 	selectedIndex := currentCount % uint64(len(sortedHops))
// 	targetFace := sortedHops[selectedIndex].Nexthop

// 	// 5. Send Immediately
// 	s.SendInterest(packet, pitEntry, targetFace, inFace)
// }

// // handlePathDiscoveryInterest implements the ported C++ logic for PD mode
// // THREAD SAFETY: Entire function logic is protected by info.mu.
// func (s *Multipath) handlePathDiscoveryInterest(
// 	packet *defn.Pkt,
// 	pitEntry table.PitEntry,
// 	inFace uint64,
// 	nexthops []*table.FibNextHopEntry,
// ) {
// 	// Name format assumption based on C++: /<prefix>/<srcNode>/pd/.../<probeIdx>/<seq>
// 	// C++: probeIdx is at get(-2)
// 	if len(packet.Name) < 2 {
// 		core.Log.Warn(s, "handlePathDiscoveryInterest: short name", "name", packet.Name)
// 		return
// 	}

// 	prefix := packet.Name[0].String()
// 	srcNode := packet.Name[1].String()

// 	// Parse Probe Index
// 	probeIdxStr := packet.Name[len(packet.Name)-2].String()
// 	probeIdx, err := strconv.Atoi(probeIdxStr)
// 	if err != nil {
// 		core.Log.Error(s, "Invalid probe index in PD Interest", "name", packet.Name)
// 		return
// 	}

// 	core.Log.Debug(s, "Received PD Interest", "name", packet.Name, "inFace", inFace, "probeIdx", probeIdx)

// 	info := s.getPathDiscoveryInfo(prefix)
// 	info.mu.Lock()
// 	defer info.mu.Unlock()

// 	// 1. Check Irrelevant Node (Pro/Con logic) & Invalid Node Flag
// 	myNodeName := s.StrategyNodeName.String()

// 	isPro := strings.HasPrefix(myNodeName, "/pro")
// 	isCon := strings.HasPrefix(myNodeName, "/con")

// 	// Normalize: myNodeName has leading "/" (e.g., "/pro0"), but prefix/srcNode from packet.Name don't (e.g., "pro0app", "con0app")
// 	// Remove leading "/" from myNodeName for comparison
// 	myNodeNameWithoutSlash := strings.TrimPrefix(myNodeName, "/")

// 	// Also normalize prefix/srcNode in case they have leading "/" (defensive)
// 	prefixNormalized := strings.TrimPrefix(prefix, "/")
// 	srcNodeNormalized := strings.TrimPrefix(srcNode, "/")

// 	// For producer: prefix is like "pro0app", node name is like "/pro0" -> "pro0"
// 	// We check if prefix starts with the node name without "/" (e.g., "pro0app" starts with "pro0")
// 	// For consumer: srcNode is like "con0app", node name is like "/con0" -> "con0"
// 	// We check if srcNode starts with the node name without "/" (e.g., "con0app" starts with "con0")
// 	var isRelevant bool
// 	if isPro {
// 		// Producer: prefixNormalized should start with myNodeNameWithoutSlash (e.g., "pro0app" starts with "pro0")
// 		isRelevant = strings.HasPrefix(prefixNormalized, myNodeNameWithoutSlash)
// 	} else if isCon {
// 		// Consumer: srcNodeNormalized should start with myNodeNameWithoutSlash (e.g., "con0app" starts with "con0")
// 		isRelevant = strings.HasPrefix(srcNodeNormalized, myNodeNameWithoutSlash)
// 	} else {
// 		// Unknown node type, treat as irrelevant
// 		isRelevant = false
// 	}

// 	if !isRelevant {
// 		// Irrelevant NFD
// 		core.Log.Debug(s, "Nack irrelevant Interest", "name", packet.Name, "myNode", myNodeName, "prefix", prefix, "srcNode", srcNode)
// 		info.IsInvalidNode = true
// 		s.SendNack(packet, pitEntry, inFace, inFace, spec.NackReasonNoRoute)
// 		return
// 	} else if info.IsInvalidNode {
// 		core.Log.Debug(s, "Node marked invalid, sending Nack", "name", packet.Name)
// 		s.SendNack(packet, pitEntry, inFace, inFace, spec.NackReasonNoRoute)
// 		return
// 	}

// 	// 2. Initialization
// 	if !info.IsInitialized {
// 		core.Log.Debug(s, "Initializing Path Discovery", "prefix", prefix, "probeIdx", probeIdx)
// 		info.IsInitialized = true
// 		info.ProbeIndex = probeIdx

// 		s.TierGrouping(info, nexthops)
// 		s.StartPathDiscovery(info)
// 	}

// 	// 3. Tier Probing Logic
// 	if !info.IsNodeConverged {
// 		if probeIdx > info.ProbeIndex {
// 			// Lock check
// 			if info.CurrentTier >= len(info.WaitList)-1 {
// 				core.Log.Error(s, "Reach the end of wait list, probe idx beyond range", "prefix", prefix)
// 				return // Stop processing
// 			} else {
// 				info.ProbeIndex = probeIdx
// 				s.ProbeNextTier(info)
// 			}
// 		} else if probeIdx == info.ProbeIndex {
// 			// Still in current tier probing
// 		} else {
// 			// probeIdx < info.ProbeIndex: Wrong case
// 			core.Log.Error(s, "Received old probeIdx", "received", probeIdx, "current", info.ProbeIndex, "prefix", prefix)
// 			return
// 		}

// 		// 4. Face Management (Frozen vs Removal)
// 		isProbed := false
// 		for _, face := range info.ProbedUpstreams {
// 			if face == inFace {
// 				isProbed = true
// 				break
// 			}
// 		}

// 		if isProbed {
// 			// Find tier index of ingress face
// 			tierIndex := -1
// 			for idx, tier := range info.WaitList {
// 				for _, hop := range tier {
// 					if hop.Nexthop == inFace {
// 						tierIndex = idx
// 						break
// 					}
// 				}
// 				if tierIndex != -1 {
// 					break
// 				}
// 			}

// 			if tierIndex != -1 {
// 				if !info.IsPdFrozen {
// 					info.IsPdFrozen = true
// 					// Add all faces in this tier to frozen lists
// 					for _, hop := range info.WaitList[tierIndex] {
// 						info.FrozenFaces[hop.Nexthop] = true
// 						info.FrozenRecoverFaces[hop.Nexthop] = true
// 					}
// 					core.Log.Debug(s, "Frozen removal later", "prefix", prefix, "triggerFace", inFace, "tier", tierIndex)
// 				}
// 			} else {
// 				core.Log.Error(s, "Cannot find ingress face in wait list", "face", inFace, "prefix", prefix)
// 				return
// 			}
// 		} else {
// 			// Execute Removal Immediately
// 			core.Log.Debug(s, "Exclude ingress face from valid/wait/converged/probed", "face", inFace, "prefix", prefix)

// 			delete(info.ValidUpstreams, inFace)

// 			// Remove from WaitList
// 			s.removeFaceFromWaitList(info, inFace)

// 			delete(info.ConvergedUpstreamTier, inFace)
// 			delete(info.ConvergedUpstreamNode, inFace)

// 			// Remove from ProbedUpstreams
// 			s.removeFaceFromProbedUpstreams(info, inFace)
// 		}

// 		// 5. Check Skip Tier
// 		if s.isCurrentTierAllInvalid(info) {
// 			s.skipTier(info)
// 		}

// 		// 6. Check No Route
// 		if len(info.ValidUpstreams) == 0 {
// 			core.Log.Debug(s, "No valid upstreams left, marking node invalid", "prefix", prefix)
// 			info.IsInvalidNode = true
// 			s.SendNack(packet, pitEntry, inFace, inFace, spec.NackReasonNoRoute)
// 			return
// 		}
// 	}

// 	// 7. Forwarding (Round-Robin directly, no queue/scheduler)
// 	if len(info.ProbedUpstreams) == 0 {
// 		// No faces to probe
// 		core.Log.Error(s, "No valid faces to probe in pd", "name", packet.Name)
// 		return
// 	}

// 	// Use ProbedSchedulerIndex for Round-Robin
// 	idx := info.ProbedSchedulerIndex
// 	if idx >= len(info.ProbedUpstreams) {
// 		idx = 0
// 	}

// 	outFace := info.ProbedUpstreams[idx]

// 	// Advance index
// 	info.ProbedSchedulerIndex = (idx + 1) % len(info.ProbedUpstreams)

// 	// Send Interest
// 	s.SendInterest(packet, pitEntry, outFace, inFace)

// 	// 8. Frozen Flag Handling (Ported from C++ PathDiscoveryScheduler)
// 	// THREAD SAFETY: Protected by info.mu
// 	if info.IsPdFrozen {
// 		// If the face we just sent to was in the frozen set, remove it.
// 		if info.FrozenFaces[outFace] {
// 			delete(info.FrozenFaces, outFace)

// 			// If no more faces are frozen, we can proceed with the delayed removal
// 			if len(info.FrozenFaces) == 0 {
// 				info.IsPdFrozen = false
// 				core.Log.Debug(s, "Unset frozen flag", "prefix", prefix)

// 				// Process the recovery list: these are faces that needed to be removed
// 				// but were held back because the state was frozen.
// 				for face := range info.FrozenRecoverFaces {
// 					// Perform the delayed removals
// 					core.Log.Debug(s, "Processing frozen recovery removal", "face", face, "prefix", prefix)
// 					delete(info.ValidUpstreams, face)
// 					s.removeFaceFromWaitList(info, face)
// 					delete(info.ConvergedUpstreamTier, face)
// 					delete(info.ConvergedUpstreamNode, face)
// 					s.removeFaceFromProbedUpstreams(info, face)
// 				}

// 				// Clear the recovery map for the next time
// 				info.FrozenRecoverFaces = make(map[uint64]bool)
// 			}
// 		}
// 	}
// }

// // ---- Helper Methods for PD Logic ----

// func (s *Multipath) TierGrouping(info *PathDiscoveryInfo, nexthops []*table.FibNextHopEntry) {
// 	// Group NextHops into tiers according to hop count (cost)
// 	costToFaces := make(map[uint64][]*table.FibNextHopEntry)
// 	var costs []uint64

// 	for _, nh := range nexthops {
// 		if _, exists := costToFaces[nh.Cost]; !exists {
// 			costs = append(costs, nh.Cost)
// 		}
// 		costToFaces[nh.Cost] = append(costToFaces[nh.Cost], nh)
// 	}

// 	// Sort costs to ensure tiers are ordered
// 	sort.Slice(costs, func(i, j int) bool { return costs[i] < costs[j] })

// 	// Build TierList
// 	info.TierList = make([][]*table.FibNextHopEntry, 0)
// 	info.TierCostList = make([]uint64, 0)

// 	for _, cost := range costs {
// 		info.TierList = append(info.TierList, costToFaces[cost])
// 		info.TierCostList = append(info.TierCostList, cost)
// 		core.Log.Debug(s, "TierGrouping", "cost", cost, "facesCount", len(costToFaces[cost]))
// 	}

// 	// Initialize WaitList as a copy of TierList
// 	info.WaitList = make([][]*table.FibNextHopEntry, len(info.TierList))
// 	copy(info.WaitList, info.TierList)

// 	info.CurrentTier = 0

// 	// Log grouping results summary
// 	totalFaces := 0
// 	for _, tier := range info.TierList {
// 		totalFaces += len(tier)
// 	}
// 	core.Log.Debug(s, "TierGrouping completed",
// 		"prefix", info.Prefix,
// 		"totalTiers", len(info.TierList),
// 		"totalFaces", totalFaces,
// 		"tierCosts", info.TierCostList)
// }

// func (s *Multipath) StartPathDiscovery(info *PathDiscoveryInfo) {
// 	// 1. Initialize valid upstream as all nexthops
// 	info.ValidUpstreams = make(map[uint64]bool)
// 	info.ConvergedUpstreamNode = make(map[uint64]bool)

// 	// Flatten TierList to get all nexthops initially
// 	for _, tier := range info.TierList {
// 		for _, nh := range tier {
// 			info.ValidUpstreams[nh.Nexthop] = true
// 			info.ConvergedUpstreamNode[nh.Nexthop] = true
// 		}
// 	}

// 	// 2. Start with tier 0
// 	info.CurrentTier = 0

// 	// 3. Remove unnecessary tiers (Cost >= 2x First Tier Cost)
// 	if len(info.TierCostList) > 0 {
// 		firstTierCost := info.TierCostList[0]
// 		thresholdCost := firstTierCost * 2

// 		core.Log.Debug(s, "StartPathDiscovery", "firstTierCost", firstTierCost, "thresholdCost", thresholdCost)

// 		// Process from highest to lowest
// 		for i := len(info.TierCostList) - 1; i > 0; i-- {
// 			if info.TierCostList[i] >= thresholdCost {
// 				core.Log.Debug(s, "Removing tier due to high cost", "tier", i, "cost", info.TierCostList[i])
// 				facesToRemove := info.WaitList[i]

// 				for _, hop := range facesToRemove {
// 					delete(info.ValidUpstreams, hop.Nexthop)
// 					delete(info.ConvergedUpstreamNode, hop.Nexthop)
// 				}

// 				// Clear tier
// 				info.WaitList[i] = []*table.FibNextHopEntry{}
// 			}
// 		}
// 	}

// 	// 4. Start probing current tier
// 	s.ProbeTier(info, info.CurrentTier)

// 	// 5. Reset scheduler index
// 	info.ProbedSchedulerIndex = 0
// }

// func (s *Multipath) ProbeTier(info *PathDiscoveryInfo, tierIndex int) {
// 	if tierIndex >= len(info.WaitList) {
// 		core.Log.Error(s, "ProbeTier: tier index out of range", "tierIndex", tierIndex, "waitListLen", len(info.WaitList))
// 		return
// 	}

// 	core.Log.Debug(s, "Probing tier", "tierIndex", tierIndex, "prefix", info.Prefix)

// 	// Add faces in this tier to ProbedUpstreams
// 	for _, hop := range info.WaitList[tierIndex] {
// 		// Add to ProbedUpstreams if not exists
// 		exists := false
// 		for _, face := range info.ProbedUpstreams {
// 			if face == hop.Nexthop {
// 				exists = true
// 				break
// 			}
// 		}
// 		if !exists {
// 			info.ProbedUpstreams = append(info.ProbedUpstreams, hop.Nexthop)
// 		}

// 		info.ConvergedUpstreamTier[hop.Nexthop] = true
// 		core.Log.Debug(s, "Start probing face", "face", hop.Nexthop)
// 	}

// 	// Node convergence for producer NFD
// 	// Check if prefix starts with node name (handling "/" prefix correctly)
// 	// Example: prefix "/pro0app" should match node name "/pro0"
// 	// Normalize both by removing leading "/" and check if prefix starts with node name
// 	if !info.IsNodeConverged {
// 		myNodeName := s.StrategyNodeName.String()
// 		myNodeNameNormalized := strings.TrimPrefix(myNodeName, "/")
// 		prefixNormalized := strings.TrimPrefix(info.Prefix, "/")
// 		if strings.HasPrefix(prefixNormalized, myNodeNameNormalized) {
// 			core.Log.Debug(s, "Node is designated pro NFD, node converged", "prefix", info.Prefix, "nodeName", myNodeName)
// 			s.localNodeConverged(info)
// 		}
// 	}
// }

// func (s *Multipath) ProbeNextTier(info *PathDiscoveryInfo) {
// 	nextTier := info.CurrentTier + 1
// 	info.CurrentTier = nextTier
// 	s.ProbeTier(info, nextTier)
// }

// func (s *Multipath) BeforeSatisfyInterest(pitEntry table.PitEntry, inFace uint64) {}

// // AfterReceiveLoopedInterest handles looped interests (when a duplicate Interest is detected)
// // Aligned with C++ logic in multipath-strategy.cpp
// // THREAD SAFETY: Stateless (safe).
// func (s *Multipath) AfterReceiveLoopedInterest(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
// 	// 1. Identify Interest Type ("pd" or "dt")
// 	// Assumption: Name format is /<prefix>/<node>/<type>/...
// 	if len(packet.Name) < 3 {
// 		core.Log.Warn(s, "AfterReceiveLoopedInterest: short name", "name", packet.Name)
// 		return
// 	}

// 	interestType := packet.Name[2].String()

// 	// ! Send NACK with "NONE" reason
// 	// Either using this way or sendnack as defined in strategy would work
// 	if interestType == "pd" {
// 		// In Go, we use the strategy helper to send the NACK
// 		s.SendNack(packet, pitEntry, inFace, inFace, spec.NackReasonNone)
// 	} else {
// 		// In Go, we just log the error as requested
// 		core.Log.Error(s, "[ERROR] Received looped interest in data transmission stage! Please check.", "name", packet.Name)
// 		return
// 	}
// }

// // AfterReceiveNack handles NACKs
// func (s *Multipath) AfterReceiveNack(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64, nackReason uint64) {
// 	// 1. Identify Interest Type ("pd" or "dt")
// 	// Assumption: Name format is /<prefix>/<node>/<type>/...
// 	if len(packet.Name) < 3 {
// 		// Not a path discovery or data transmission Interest, use default behavior
// 		core.Log.Warn(s, "AfterReceiveNack: short name, using default behavior", "name", packet.Name)
// 		s.forwardNackDefault(packet, pitEntry, inFace, nackReason)
// 		return
// 	}

// 	interestType := packet.Name[2].String()

// 	switch interestType {
// 	case "pd":
// 		s.handlePathDiscoveryNack(packet, pitEntry, inFace, nackReason)
// 	case "dt":
// 		s.handleTransmissionNack(packet, pitEntry, inFace, nackReason)
// 	default:
// 		// Unknown type, use default behavior
// 		core.Log.Warn(s, "AfterReceiveNack: unknown type, using default behavior", "type", interestType, "name", packet.Name)
// 		s.forwardNackDefault(packet, pitEntry, inFace, nackReason)
// 	}
// }

// // forwardNackDefault forwards NACK to all downstream faces (standard NFD behavior)
// func (s *Multipath) forwardNackDefault(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64, nackReason uint64) {
// 	// Forward NACK to all downstream faces (in-records) except the incoming face
// 	for faceID := range pitEntry.InRecords() {
// 		if faceID != inFace {
// 			s.SendNack(packet, pitEntry, faceID, inFace, nackReason)
// 		}
// 	}
// }

// // handlePathDiscoveryNack processes NACKs for path discovery Interests
// // THREAD SAFETY: Protected by info.mu
// func (s *Multipath) handlePathDiscoveryNack(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64, nackReason uint64) {
// 	// 1. Lookup FIB to get Prefix
// 	prefix := "/"
// 	if len(packet.Name) > 0 {
// 		prefix = packet.Name[0].String()
// 	}

// 	info := s.getPathDiscoveryInfo(prefix)
// 	info.mu.Lock()
// 	defer info.mu.Unlock()

// 	core.Log.Debug(s, "Path Discovery NACK received", "name", packet.Name, "reason", nackReason, "inFace", inFace, "prefix", prefix)

// 	if nackReason == spec.NackReasonNoRoute {
// 		// Check invalid node flag
// 		if info.IsInvalidNode {
// 			s.forwardNackDefault(packet, pitEntry, inFace, nackReason)
// 			return
// 		}

// 		// Exclude invalid upstream faces if not frozen
// 		if !info.IsPdFrozen {
// 			core.Log.Debug(s, "Exclude ingress face from valid/wait/converged/probed due to NACK", "face", inFace, "prefix", prefix)
// 			// 1. Remove from ValidUpstreams
// 			delete(info.ValidUpstreams, inFace)

// 			// 2. Remove from WaitList (using helper)
// 			s.removeFaceFromWaitList(info, inFace)

// 			// 3. Remove from ConvergedUpstreamTier
// 			delete(info.ConvergedUpstreamTier, inFace)

// 			// 4. Remove from ConvergedUpstreamNode
// 			delete(info.ConvergedUpstreamNode, inFace)

// 			// 5. Remove from ProbedUpstreams (using helper)
// 			s.removeFaceFromProbedUpstreams(info, inFace)
// 		} else {
// 			core.Log.Debug(s, "Frozen flag is set, ignore nack", "face", inFace, "prefix", prefix)
// 		}

// 		// Check skip tier
// 		// If all faces in the current tier are invalid, we skip the rest of the tiers and converge
// 		if s.isCurrentTierAllInvalid(info) {
// 			s.skipTier(info)
// 		}

// 		// Check whether there's no valid upstream left
// 		if len(info.ValidUpstreams) == 0 {
// 			core.Log.Debug(s, "Invalid node, send nack to disable this node", "prefix", prefix)
// 			info.IsInvalidNode = true
// 			s.forwardNackDefault(packet, pitEntry, inFace, spec.NackReasonNoRoute)
// 			return
// 		}

// 		// Send NACK with "NONE" reason
// 		// This suppresses the NO_ROUTE NACK to downstream if we still have valid options
// 		s.forwardNackDefault(packet, pitEntry, inFace, spec.NackReasonNone)
// 		return

// 	} else if nackReason == spec.NackReasonNone {
// 		// Propagate NONE
// 		s.forwardNackDefault(packet, pitEntry, inFace, spec.NackReasonNone)
// 	}
// }

// // handleTransmissionNack processes NACKs for data transmission Interests
// // THREAD SAFETY: Protected by info.mu
// func (s *Multipath) handleTransmissionNack(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64, nackReason uint64) {
// 	// 1. Lookup FIB to get Prefix
// 	prefix := "/"
// 	if len(packet.Name) > 0 {
// 		prefix = packet.Name[0].String()
// 	}

// 	info := s.getPathDiscoveryInfo(prefix)
// 	info.mu.Lock()
// 	defer info.mu.Unlock()

// 	core.Log.Info(s, "Data Transmission NACK received", "name", packet.Name, "reason", nackReason, "inFace", inFace, "prefix", prefix)

// 	// 2. Handle NACK based on reason
// 	switch nackReason {
// 	// case spec.NackReasonCongestion:
// 	// 	// Congestion NACK - mark face but don't remove from active faces immediately
// 	// 	// TODO: Implement congestion control logic
// 	// 	core.Log.Debug(s, "Congestion NACK on face", "face", inFace, "prefix", prefix)

// 	case spec.NackReasonNoRoute:
// 		// No route NACK - remove from active faces
// 		// Remove from active faces list
// 		for i, activeFace := range info.ActiveFaces {
// 			if activeFace == inFace {
// 				info.ActiveFaces = append(info.ActiveFaces[:i], info.ActiveFaces[i+1:]...)
// 				// Adjust index if needed
// 				if info.ActiveFaceIndex >= len(info.ActiveFaces) {
// 					info.ActiveFaceIndex = 0
// 				}
// 				break
// 			}
// 		}
// 		core.Log.Debug(s, "NoRoute NACK - removed from active faces", "face", inFace, "prefix", prefix)

// 	// case spec.NackReasonDuplicate:
// 	// 	// Duplicate NACK - usually safe to ignore, but log it
// 	// 	core.Log.Trace(s, "Duplicate NACK received", "face", inFace, "prefix", prefix)

// 	default:
// 		// Other NACK reasons
// 		core.Log.Debug(s, "Other NACK reason", "reason", nackReason, "face", inFace, "prefix", prefix)
// 	}

// 	// 3. Forward NACK downstream
// 	s.forwardNackDefault(packet, pitEntry, inFace, nackReason)
// }

// // THREAD SAFETY: Protected by info.mu
// func (s *Multipath) handlePathDiscoveryData(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
// 	// 1. Lookup FIB to get Prefix
// 	prefix := "/"
// 	if len(packet.Name) > 0 {
// 		prefix = packet.Name[0].String()
// 	}

// 	info := s.getPathDiscoveryInfo(prefix)

// 	// Decode the full packet to access the internal Content.
// 	// This is stateless and safe.
// 	parsedData, _, err := spec.Spec{}.ReadData(enc.NewWireView(packet.Raw))
// 	if err != nil {
// 		core.Log.Error(s, "Failed to decode Data packet", "err", err, "name", packet.Name)
// 		// Forward anyway for robustness
// 		for faceID := range pitEntry.InRecords() {
// 			s.SendData(packet, pitEntry, faceID, inFace)
// 		}
// 		return
// 	}

// 	// Deserialize Metadata using ONLY the payload
// 	dp, err := DeserializeDiscovery(parsedData.Content().Join())
// 	if err != nil {
// 		core.Log.Error(s, "Failed to deserialize DiscoveryPacket", "err", err)
// 		return
// 	}

// 	info.mu.Lock()
// 	defer info.mu.Unlock()

// 	core.Log.Debug(s, "Received PD Data", "name", packet.Name, "inFace", inFace, "TierConverged", dp.TierConverged, "NodeConverged", dp.NodeConverged)

// 	// 3. Process Convergence Logic
// 	if dp.TierConverged {
// 		if !info.IsPdFrozen {
// 			core.Log.Debug(s, "Removing converged upstream tier", "face", inFace, "prefix", prefix)
// 			delete(info.ConvergedUpstreamTier, inFace)
// 		} else {
// 			core.Log.Debug(s, "Frozen flag is set, ignore data for convergence", "face", inFace, "prefix", prefix)
// 		}

// 		if len(info.ConvergedUpstreamTier) == 0 {
// 			dp.TierConverged = true
// 			core.Log.Debug(s, "Tier converged", "tier", info.CurrentTier, "prefix", prefix)
// 			// Check if node converged
// 			if !info.IsNodeConverged && s.areHigherTiersEmpty(info) {
// 				s.localNodeConverged(info)
// 			}
// 		} else {
// 			dp.TierConverged = false
// 		}
// 	}

// 	// Check if this is a producer node (starts with "pro")
// 	nodeName := s.StrategyNodeName.String()
// 	if strings.HasPrefix(nodeName, "/pro") {
// 		dp.TierConverged = true
// 	}

// 	if dp.NodeConverged {
// 		core.Log.Debug(s, "Upstream node converged", "face", inFace, "prefix", prefix)
// 		delete(info.ConvergedUpstreamNode, inFace)

// 		if info.IsNodeConverged && len(info.ConvergedUpstreamNode) == 0 {
// 			dp.NodeConverged = true
// 			core.Log.Debug(s, "Node converged", "prefix", prefix)
// 		} else {
// 			dp.NodeConverged = false
// 		}
// 	} else {
// 		dp.NodeConverged = false
// 	}

// 	core.Log.Debug(s, "Sent new PD Data", "name", packet.Name, "inFace", inFace, "TierConverged", dp.TierConverged, "NodeConverged", dp.NodeConverged)

// 	// 4. Serialize updated metadata
// 	newContent, err := dp.SerializeDiscovery()
// 	if err != nil {
// 		core.Log.Error(s, "Failed to serialize DiscoveryPacket", "err", err)
// 		return
// 	}

// 	// Re-construct and Re-encode the Packet
// 	data, ok := parsedData.(*spec.Data)
// 	if !ok {
// 		core.Log.Error(s, "Failed to type assert Data packet")
// 		return
// 	}

// 	data.ContentV = enc.Wire{newContent}

// 	packetEncoder := spec.PacketEncoder{}
// 	packetEncoder.Init(&spec.Packet{Data: data})
// 	newWire := packetEncoder.Encode(&spec.Packet{Data: data})

// 	packet.Raw = newWire

// 	for faceID := range pitEntry.InRecords() {
// 		s.SendData(packet, pitEntry, faceID, inFace)
// 	}
// }

// // TODO: We need to implement the "dt" mode congestion control later
// // handleTransmissionData processes "dt" (DT) packets
// // THREAD SAFETY: Stateless (safe).
// func (s *Multipath) handleTransmissionData(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
// 	// 1. Decode the full packet to access the internal Content
// 	parsedData, _, err := spec.Spec{}.ReadData(enc.NewWireView(packet.Raw))
// 	if err != nil {
// 		core.Log.Error(s, "Failed to decode Data packet", "err", err, "name", packet.Name)
// 		// Forward anyway for robustness
// 		for faceID := range pitEntry.InRecords() {
// 			s.SendData(packet, pitEntry, faceID, inFace)
// 		}
// 		return
// 	}

// 	// 2. Extract Metadata using DataPacket from datapacket.go
// 	originalContent := parsedData.Content().Join()
// 	dtPacket, err := DeserializeData(originalContent)
// 	if err != nil {
// 		// Log warning but allow forwarding (robustness)
// 		core.Log.Warn(s, "Failed to deserialize DataPacket", "err", err, "name", packet.Name)
// 	} else {
// 		// Test serialization: Serialize the deserialized packet back to verify it works correctly
// 		serializedContent, err := dtPacket.SerializeData()
// 		if err != nil {
// 			core.Log.Warn(s, "Failed to serialize DataPacket", "err", err, "name", packet.Name)
// 		} else {
// 			// Verify that serialized content matches original content
// 			if len(serializedContent) != len(originalContent) {
// 				core.Log.Warn(s, "Serialization size mismatch",
// 					"original", len(originalContent),
// 					"serialized", len(serializedContent),
// 					"name", packet.Name)
// 			} else {
// 				// Compare serialized content with original content
// 				if !bytes.Equal(originalContent, serializedContent) {
// 					core.Log.Warn(s, "Serialization content mismatch", "name", packet.Name)
// 				} else {
// 					// Success - deserialization and serialization work correctly
// 					core.Log.Trace(s, "DataPacket serialization test passed", "name", packet.Name)
// 				}
// 			}
// 		}
// 		// TODO: Implement Sliding Window (SW) logic here if needed using dtPacket.InterestQsf / dtPacket.DataQsf
// 	}

// 	// Extract sequence number from data name and log every 200 packets
// 	dataName := packet.Name.String()
// 	seqNum := extractSeqNum(dataName)
// 	if seqNum >= 0 && seqNum%200 == 0 {
// 		core.Log.Info(s, "Data Forwarded",
// 			"name", dataName,
// 			"seq", seqNum)
// 	}

// 	// 2. Bandwidth Estimation & Congestion Control (Placeholder)
// 	// s.FaceBandwidthEstimation(inFace)
// 	// s.FaceSendRateEstimation(inFace)

// 	// 3. Forwarding Logic
// 	for faceID := range pitEntry.InRecords() {
// 		// If we needed to update QSF values per downstream:
// 		// 1. Modify dtPacket.InterestQsf / dtPacket.DataQsf
// 		// 2. newContent, _ := dtPacket.SerializeData()
// 		// 3. packet.Content = newContent
// 		s.SendData(packet, pitEntry, faceID, inFace)
// 	}
// }

// // ---- Helper Functions ----

// // areHigherTiersEmpty checks if tiers > currentTier are empty
// func (s *Multipath) areHigherTiersEmpty(info *PathDiscoveryInfo) bool {
// 	for i := info.CurrentTier + 1; i < len(info.WaitList); i++ {
// 		if len(info.WaitList[i]) > 0 {
// 			return false
// 		}
// 	}
// 	return true
// }

// // isCurrentTierAllInvalid checks if all faces in the current tier are invalid
// func (s *Multipath) isCurrentTierAllInvalid(info *PathDiscoveryInfo) bool {
// 	if info.CurrentTier >= len(info.TierList) {
// 		return true
// 	}

// 	// Check if any face in the current tier (from static TierList) is still in ValidUpstreams
// 	currentTierFaces := info.TierList[info.CurrentTier]
// 	for _, hop := range currentTierFaces {
// 		if info.ValidUpstreams[hop.Nexthop] {
// 			return false // Found a valid one
// 		}
// 	}
// 	return true // All invalid
// }

// // skipTier clears remaining waitlists and forces convergence
// func (s *Multipath) skipTier(info *PathDiscoveryInfo) {
// 	// Erase all faces from upper tiers of waitlist (currentTier and above)
// 	// Also remove them from ValidUpstreams and ConvergedUpstreamNode
// 	for i := info.CurrentTier; i < len(info.WaitList); i++ {
// 		facesToRemove := info.WaitList[i]

// 		// Clear the tier in WaitList
// 		info.WaitList[i] = []*table.FibNextHopEntry{}

// 		for _, hop := range facesToRemove {
// 			// Remove from ValidUpstreams
// 			delete(info.ValidUpstreams, hop.Nexthop)
// 			// Remove from ConvergedUpstreamNode
// 			delete(info.ConvergedUpstreamNode, hop.Nexthop)
// 		}
// 	}

// 	// Set node as converged
// 	core.Log.Info(s, "Skipped tier triggered", "prefix", info.Prefix, "tier", info.CurrentTier)
// 	if !info.IsNodeConverged {
// 		s.localNodeConverged(info)
// 	}

// }

// // localNodeConverged handles the logic when the local node converges
// // THREAD SAFETY: Must be called with info.mu.Lock() held by the caller
// // With shared multipathPdState across all threads, this ensures only one thread
// // can trigger convergence for a given prefix, preventing duplicate convergence calls
// func (s *Multipath) localNodeConverged(info *PathDiscoveryInfo) {
// 	// Double-check pattern: even though caller should have lock, this prevents
// 	// duplicate work if somehow called multiple times (defensive programming)
// 	if !info.IsNodeConverged {
// 		// Clear converged upstream tier set
// 		info.ConvergedUpstreamTier = make(map[uint64]bool)
// 		info.IsNodeConverged = true

// 		// Update Tier List based on Wait List (Finalizing Valid Upstreams)
// 		s.updateAfterPathDiscovery(info)

// 		// TODO: Activate Data Transmission Scheduler
// 		// TODO: Implement dt initialization logic later
// 		// s.activateDTScheduler(info)

// 		core.Log.Info(s, "Local Node Converged", "prefix", info.Prefix)
// 	}
// }

// // updateAfterPathDiscovery rebuilds the TierList based on valid results
// func (s *Multipath) updateAfterPathDiscovery(info *PathDiscoveryInfo) {
// 	var newTierList [][]*table.FibNextHopEntry
// 	var newCostList []uint64

// 	for i, tier := range info.WaitList {
// 		if len(tier) > 0 {
// 			newTierList = append(newTierList, tier)
// 			if i < len(info.TierCostList) {
// 				newCostList = append(newCostList, info.TierCostList[i])
// 			}
// 		}
// 	}

// 	info.TierList = newTierList
// 	info.TierCostList = newCostList
// }

// // Helper to remove face from WaitList
// func (s *Multipath) removeFaceFromWaitList(info *PathDiscoveryInfo, face uint64) {
// 	for i := range info.WaitList {
// 		var newTier []*table.FibNextHopEntry
// 		for _, hop := range info.WaitList[i] {
// 			if hop.Nexthop != face {
// 				newTier = append(newTier, hop)
// 			}
// 		}
// 		info.WaitList[i] = newTier
// 	}
// }

// // Helper to remove face from ProbedUpstreams
// func (s *Multipath) removeFaceFromProbedUpstreams(info *PathDiscoveryInfo, face uint64) {
// 	for i, f := range info.ProbedUpstreams {
// 		if f == face {
// 			info.ProbedUpstreams = append(info.ProbedUpstreams[:i], info.ProbedUpstreams[i+1:]...)
// 			break
// 		}
// 	}
// }
