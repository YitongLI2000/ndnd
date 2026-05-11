package fw

import (
	"encoding/csv"
	"math"
	"os"
	"path/filepath"
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
	Packet         *defn.Pkt
	PitEntry       table.PitEntry
	InFace         uint64
	OwnerThreadID  int
	CandidateFaces []uint64
	AssignedFace   uint64
	EnqueueTime    time.Time
}

// hopSnapshot is an immutable copy of FIB nexthop metadata used by PD state.
type hopSnapshot struct {
	Nexthop uint64
	Cost    uint64
}

// DtRateControlParams keeps all DT face-level rate control hyperparameters in one place.
// Units:
// - Queue parameters are normalized occupancy ratios in [0, 1].
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
	dtBaseThroughputWindow = 20 * time.Millisecond //! Sliding window duration
	dtMinThroughputWindow  = 1 * time.Millisecond
	dtMaxExpectedPps       = 100000.0
	dtWindowSafetyFactor   = 2.0
	dtSlopeTauRatio        = 1.0 / 3.0
	// Set to false to use RTT bootstrap: clamp(dtControlPeriodRttScale * avg(first 10 RTT samples)).
	dtUseStaticControlPeriod = true
	dtStaticControlPeriod    = 20 * time.Millisecond
	// Enable startup InterestQsf/DataQsf bias learning/removal on the forwarder.
	dtUseQsfBiasRemoval     = false //! This feature should be tested
	dtControlRttInitSamples = 10
	// DT per-face control period = dtControlPeriodRttScale * avg bootstrap RTT.
	dtControlPeriodRttScale = 1.0
	dtControlPeriodMin      = 10 * time.Millisecond
	dtControlPeriodMax      = 30 * time.Millisecond
	dtControllerDebugRounds = 200
	// During partial bootstrap, occasionally probe unready faces while shaping ready faces.
	dtPartialBootstrapProbeEvery = 8
	// Emit scheduler execution debug summary every N per-face ticks per thread.
	dtSchedulerDebugEveryTicks = 200
	// Emit a per-thread DT consume warning only when granted budget is not fully
	// consumed. This keeps logs focused on possible async over-grant cases.
	dtSchedulerConsumeDebug = true
	// Per-thread work caps applied on each fast scheduler tick.
	dtSchedulerMaxFacesPerTick = 16
	dtSchedulerMaxSendPerTick  = 64
	dtThreadQuotaCapPackets    = 512
	// Keep a small equal-share baseline so backlog-aware distribution does not
	// completely starve lightly loaded active threads.
	dtThreadActiveBaseWeight = 1.0
	// Retain a small fractional fairness carry across service epochs so integer
	// quota rounding stays fair without requiring sub-packet sends.
	dtThreadQuotaCarryCapPackets = 1.0
	// Liveness guard: when a face has pending demand but measured send is 0 in one
	// control round, grant a tiny forced-send budget on upcoming ticks.
	dtLivenessForcedPacketsPerStarvedRound = 2
	dtLivenessForcedPacketsMax             = 32
	dtLivenessStarvationLogThreshold       = 3
	legacyDtFaceInitRatePps                = 700.0
	legacyDtFaceInitPacketBytes            = 16 + (750 * 8)
	targetDtFaceInitMbps                   = (legacyDtFaceInitRatePps * legacyDtFaceInitPacketBytes * 8.0) / 1e6
	dtFaceTokenBurstCapPackets             = 64.0
	dtRetainedBudgetCapPackets             = 1.0
	dtBandwidthNoSampleWarnInterval        = 500 * time.Millisecond
	//! Per-thread logical DT Interest queue capacity used to normalize InterestQsf.
	dtInterestQueueCapacityPackets = 20.0
	// QSF smoothing uses only the latest few in-window samples even if the
	// throughput window itself is larger for bandwidth estimation.
	dtQsfSampleLimit = 3
	dtBwSampleMin    = 2
	//! Bw estimation
	dtBwObsLimit   = 3
	dtBwAlphaUp    = 0.25
	dtBwAlphaDown  = 0.25
	dtBwAlphaSmall = 0.10
	// Emit per-face DT control-round CSVs for consumer forwarders into the Mars
	// logs directory. Files are named like "con0_face6.csv".
	dtFaceStatsCsvDir = "std/examples/mars/logs"
	// Mininet/netem qdisc packet limit used to normalize qdisc-derived DataQsf.
	dtQdiscQueueCapacityPackets = 1000.0
	// Link-service queue capacity used when DT metadata sources the ndnd send queue.
	dtLinkServiceQueueCapacityPackets = 20.0
	// DataQsf source for outgoing DT metadata: "qdisc", "transport", or "link-service".
	dtDataQsfSource          = "qdisc"
	dtFailureModeNormal      = "normal"
	dtFailureModeFaceDisable = "face-disable"
)

var dtRateControlParams = DtRateControlParams{
	MaxFwdQueueSize:   1.0,
	FwdQueueAlpha:     0.5,
	FwdQueueBeta:      0.8,
	FwdMDFactor:       0.9,
	FwdRPFactor:       1.05,
	FwdGDFactor:       0.95,
	FwdCIFactor:       1.02,
	FwdSlopeThreshold: 0.1,
	MaxBWSafetyRatio:  1.3,
	MinBWSafetyRatio:  0.5,
	FwdMinRate:        1,   // unit: mbps
	FwdMaxRate:        0.0, // 0 means "disabled"
}

var (
	dtThroughputWindowDur  = dtBaseThroughputWindow
	dtMaxSamplesPerFace    = int(dtMaxExpectedPps * dtBaseThroughputWindow.Seconds() * dtWindowSafetyFactor)
	dtDataPacketSizeBytes  = 0
	dtFaceInitRate         = legacyDtFaceInitRatePps
	dtEstimatorTunableOnce sync.Once
	dtResolvedLogDir       string
	dtLogDirOnce           sync.Once
	dtFailureConfigOnce    sync.Once
	dtResolvedFailureCfg   dtFailureConfig
	dtFailureNodeOnce      sync.Once
	dtResolvedFailureNode  *dtFailureNodeState
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

func dtLogFloat1(v float64) float64 {
	return math.Round(v*10) / 10
}

func dtRatePpsToMbps(ratePps float64) float64 {
	if ratePps <= 0 {
		return 0
	}
	packetBytes := dtDataPacketSizeBytes
	if packetBytes <= 0 {
		packetBytes = datapacket.NewDataPacket().GetSize()
	}
	if packetBytes <= 0 {
		return 0
	}
	return (ratePps * float64(packetBytes) * 8.0) / 1e6
}

func dtRateMbpsToPps(rateMbps float64) float64 {
	if rateMbps <= 0 {
		return 0
	}
	packetBytes := dtDataPacketSizeBytes
	if packetBytes <= 0 {
		packetBytes = datapacket.NewDataPacket().GetSize()
	}
	if packetBytes <= 0 {
		return 0
	}
	return (rateMbps * 1e6) / (float64(packetBytes) * 8.0)
}

func dtSendLimitReason(ratePps float64, measuredPps float64, pending int) string {
	gap := ratePps - measuredPps
	tolerance := math.Max(1.0, ratePps*0.1)
	if gap <= -tolerance {
		return "send-burst"
	}
	if math.Abs(gap) <= tolerance {
		return "rate-matched"
	}
	if pending <= 0 {
		return "iq-empty"
	}
	return "scheduler-limited"
}

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

func dtFaceStatsDir() string {
	dtLogDirOnce.Do(func() {
		if envDir := strings.TrimSpace(os.Getenv("MARS_LOG_DIR")); envDir != "" {
			dtResolvedLogDir = envDir
		} else {
			dtResolvedLogDir = dtFaceStatsCsvDir
		}
	})
	return dtResolvedLogDir
}

func conditionDtDataQsf(source string, raw float64) float64 {
	_ = source
	if raw < 0 || math.IsNaN(raw) {
		return 0
	}
	if raw > 1 {
		return 1
	}
	return raw
}

func normalizeDtQueueSignal(raw float64, capacity float64) float64 {
	if raw <= 0 || capacity <= 0 || math.IsNaN(raw) || math.IsNaN(capacity) {
		return 0
	}
	return conditionDtDataQsf("", raw/capacity)
}

func correctedDtQsf(raw float64, bias float64) float64 {
	if !dtUseQsfBiasRemoval {
		return raw
	}
	corrected := raw - bias
	if corrected < 0 {
		return 0
	}
	return corrected
}

func dtGlobalInterestQueueCapacityPackets() float64 {
	nThreads := CfgNumThreads()
	if nThreads < 1 {
		nThreads = 1
	}
	return dtInterestQueueCapacityPackets * float64(nThreads)
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
	SampleCount   int
	LastUpdated   time.Time
}

type dtFaceInterestSignalState struct {
	mu     sync.RWMutex
	byPref map[string]dtPrefixInterestSignal
}

type dtFaceControlState struct {
	mu                sync.Mutex
	activated         bool
	rateControlActive bool
	bwCapActive       bool
	period            time.Duration
	nextRun           time.Time
	rate              float64
	action            DtRateControlAction
	tokens            float64
	lastRefill        time.Time
	dataQsfBias       float64
	dataQsfBiasReady  bool
	// Aggregate sent packet counter for per-face actual send-rate estimation.
	sentCounter              uint64
	lastRateSampleCount      uint64
	lastRateSampleTime       time.Time
	lastInterestQ            float64
	lastInterestSlope        float64
	lastDataQ                float64
	lastDataSlope            float64
	lastSignalPrefixes       int
	lastSignalReady          bool
	lastRoundReady           bool
	lastRoundSignalQ         float64
	lastRoundSignalSlope     float64
	lastRoundSignalType      string
	lastRoundInterestQ       float64
	lastRoundInterestSlope   float64
	lastRoundDataQ           float64
	lastRoundDataSlope       float64
	lastRoundEstimatedBW     float64
	lastRoundMeasuredBW      float64
	lastRoundBwSamples       int
	lastRoundBwRule          string
	lastRoundMeasuredSendPps float64
	lastRoundPrevRate        float64
	lastRoundNewRate         float64
	lastRoundAction          DtRateControlAction
	// Consecutive control rounds with pending demand but zero measured send rate.
	starvationZeroSendRounds uint64
	// Forced packets to inject in upcoming scheduler ticks for liveness.
	livenessForcedPackets    int
	demandByThread           []int
	budgetByThread           []int
	carryByThread            []float64
	roundGrantedByThread     []uint64
	roundUsedByThread        []uint64
	roundZeroBudgetByThread  []uint64
	roundEmptyBudgetByThread []uint64
	roundSendFailByThread    []uint64
	roundPendingSumByThread  []uint64
	roundPendingMaxByThread  []int
	roundVisitsByThread      []uint64
	roundMintedTokens        float64
	budgetRR                 int
	budgetRounds             uint64
	csvSink                  *dtFaceCsvSink
}

type dtFaceBandwidthState struct {
	mu               sync.Mutex
	samples          []time.Time
	observations     []float64
	estimatedPps     float64
	lastUpdated      time.Time
	lastNoSampleWarn time.Time
	lastUpdateRule   string
	lastMeasuredPps  float64
	lastObsTime      time.Time
	bootstrapReady   bool
	capEligible      bool
}

type dtGlobalPrefixQueueState struct {
	length atomic.Int64
}

type dtThreadFaceExecState struct {
	prefixRR      uint64
	tickCount     uint64
	sentBootstrap uint64
	sentShaped    uint64
	emptyDequeues uint64
	tokenMisses   uint64
	sendAgeTotal  time.Duration
	sendAgeMax    time.Duration
	staleSends    uint64
}

type dtFaceCsvSink struct {
	mu            sync.Mutex
	path          string
	file          *os.File
	writer        *csv.Writer
	headerWritten bool
}

type dtFailureConfig struct {
	Enabled          bool
	Mode             string
	StartOffset      time.Duration
	EndOffset        time.Duration
	ExpectedPrefixes int
}

type dtFailureNodeState struct {
	mu                sync.Mutex
	convergedPrefixes map[string]bool
	candidateFaces    map[uint64]bool
	armed             bool
	active            bool
	recovered         bool
	startTime         time.Time
	endTime           time.Time
	failedFace        uint64
	activationEpoch   uint64
}

type dtFailureSnapshot struct {
	Enabled         bool
	Active          bool
	FailedFace      uint64
	ActivationEpoch uint64
}

type dtLocalPrefixQueue struct {
	prefix string
	queue  []*InterestQueueItem
}

type dtPrefixFaceAssignState struct {
	credits     map[uint64]float64
	assignCount map[uint64]uint64
	logCount    uint64
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
	// ---- Scheduler ----
	Ticker     *time.Ticker
	StopTicker chan struct{}

	// ---- Data Transmission ----
	ActiveFaces     []uint64
	ActiveFaceIndex int
	// DT forwarding mode after PD converges for this prefix.
	DtValidFaces             []uint64
	DtFaceRateControlEnabled bool
	// Bootstrap RR index for immediate DT forwarding before per-face shaping is fully ready.
	DtBootstrapFaceRR       int
	DtBootstrapProbeCounter int
	// Placeholder states for per-upstream DT congestion control.
	// Update logic of these metrics is handled by separate steps.
	UpstreamQsfInterest          map[uint64]float64
	UpstreamQsfData              map[uint64]float64
	UpstreamQsfInterestSlope     map[uint64]float64
	UpstreamQsfDataSlope         map[uint64]float64
	UpstreamInterestQsfBias      map[uint64]float64
	UpstreamInterestQsfBiasReady map[uint64]bool
	UpstreamSamples              map[uint64][]DtUpstreamSample
}

// Shared state across all Multipath strategy instances (all threads)
// This ensures that PathDiscoveryInfo is shared across threads, not duplicated per thread
var multipathPdState sync.Map
var dtRttBootstrapByFace sync.Map
var dtFaceInterestSignalByFace sync.Map
var dtFaceControlByFace sync.Map
var dtFaceBandwidthByFace sync.Map
var dtGlobalPrefixQueueByPrefix sync.Map
var dtFastSchedulerMu sync.Mutex
var dtFastSchedulerNext time.Time
var dtFastSchedulerEpoch uint64
var dtFaceCsvStartMu sync.Mutex
var dtFaceCsvStart time.Time

// Multipath Strategy Struct
type Multipath struct {
	StrategyBase
	dtExecByFace          map[uint64]*dtThreadFaceExecState
	dtQueuesByPrefix      map[string]*dtLocalPrefixQueue
	dtAssignByPrefix      map[string]*dtPrefixFaceAssignState
	dtLocalFacePrefixes   map[uint64][]string
	dtLocalFaceKnown      map[uint64]map[string]bool
	dtLocalFaceOrder      []uint64
	dtLocalFaceSeen       map[uint64]bool
	dtLocalFaceRR         uint64
	dtFailureAppliedEpoch uint64
}

func init() {
	strategyInit = append(strategyInit, func() Strategy { return &Multipath{} })
	StrategyVersions["multipath"] = []uint64{1}
	initDtEstimatorTunables()
}

func (s *Multipath) Instantiate(fwThread *Thread) {
	s.NewStrategyBase(fwThread, "multipath", 1)
	s.dtExecByFace = make(map[uint64]*dtThreadFaceExecState)
	s.dtQueuesByPrefix = make(map[string]*dtLocalPrefixQueue)
	s.dtAssignByPrefix = make(map[string]*dtPrefixFaceAssignState)
	s.dtLocalFacePrefixes = make(map[uint64][]string)
	s.dtLocalFaceKnown = make(map[uint64]map[string]bool)
	s.dtLocalFaceSeen = make(map[uint64]bool)
}

// Helper to get or create the state for a prefix
// THREAD SAFETY: Uses package-level multipathPdState shared across all threads
func (s *Multipath) getPathDiscoveryInfo(prefix string) *PathDiscoveryInfo {
	val, loaded := multipathPdState.LoadOrStore(prefix, &PathDiscoveryInfo{
		Prefix:                       prefix,
		ConvergedUpstreamTier:        make(map[uint64]bool),
		ConvergedUpstreamNode:        make(map[uint64]bool),
		ValidUpstreams:               make(map[uint64]bool),
		FrozenFaces:                  make(map[uint64]bool),
		FrozenRecoverFaces:           make(map[uint64]bool),
		PdInterestQueue:              make([]*InterestQueueItem, 0),
		UpstreamQsfInterest:          make(map[uint64]float64),
		UpstreamQsfData:              make(map[uint64]float64),
		UpstreamQsfInterestSlope:     make(map[uint64]float64),
		UpstreamQsfDataSlope:         make(map[uint64]float64),
		UpstreamInterestQsfBias:      make(map[uint64]float64),
		UpstreamInterestQsfBiasReady: make(map[uint64]bool),
		UpstreamSamples:              make(map[uint64][]DtUpstreamSample),
		StopTicker:                   make(chan struct{}),
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

func (s *Multipath) dtNodeNameNormalized() string {
	return strings.TrimPrefix(s.StrategyNodeName.String(), "/")
}

func getDtFailureConfig() dtFailureConfig {
	dtFailureConfigOnce.Do(func() {
		cfg := dtFailureConfig{
			Mode:             dtFailureModeNormal,
			ExpectedPrefixes: 5,
		}
		mode := strings.TrimSpace(os.Getenv("MARS_FAILURE_MODE"))
		if mode == "" {
			dtResolvedFailureCfg = cfg
			return
		}
		cfg.Mode = mode
		cfg.Enabled = mode == dtFailureModeFaceDisable
		if startMs, err := strconv.Atoi(strings.TrimSpace(os.Getenv("MARS_FAILURE_START_MS"))); err == nil && startMs >= 0 {
			cfg.StartOffset = time.Duration(startMs) * time.Millisecond
		}
		if endMs, err := strconv.Atoi(strings.TrimSpace(os.Getenv("MARS_FAILURE_END_MS"))); err == nil && endMs > 0 {
			cfg.EndOffset = time.Duration(endMs) * time.Millisecond
		}
		if prefixes, err := strconv.Atoi(strings.TrimSpace(os.Getenv("MARS_FAILURE_PREFIX_COUNT"))); err == nil && prefixes > 0 {
			cfg.ExpectedPrefixes = prefixes
		}
		if cfg.EndOffset <= cfg.StartOffset {
			cfg.Enabled = false
		}
		dtResolvedFailureCfg = cfg
	})
	return dtResolvedFailureCfg
}

func getDtFailureNodeState() *dtFailureNodeState {
	dtFailureNodeOnce.Do(func() {
		dtResolvedFailureNode = &dtFailureNodeState{
			convergedPrefixes: make(map[string]bool),
			candidateFaces:    make(map[uint64]bool),
		}
	})
	return dtResolvedFailureNode
}

func dtFailureEligibleNode(nodeName string) bool {
	return strings.HasPrefix(nodeName, "con")
}

func dtFilterFacesWithoutFailure(candidateFaces []uint64, blockedFace uint64) []uint64 {
	if blockedFace == 0 || len(candidateFaces) == 0 {
		return append([]uint64(nil), candidateFaces...)
	}
	filtered := make([]uint64, 0, len(candidateFaces))
	for _, face := range candidateFaces {
		if face == blockedFace {
			continue
		}
		filtered = append(filtered, face)
	}
	return filtered
}

func dtSortedFaceKeys(faceSet map[uint64]bool) []uint64 {
	faces := make([]uint64, 0, len(faceSet))
	for face := range faceSet {
		faces = append(faces, face)
	}
	sort.Slice(faces, func(i, j int) bool {
		return faces[i] < faces[j]
	})
	return faces
}

func (s *Multipath) armDtFailureAfterPdConvergence(info *PathDiscoveryInfo, now time.Time) {
	cfg := getDtFailureConfig()
	nodeName := s.dtNodeNameNormalized()
	if !cfg.Enabled || !dtFailureEligibleNode(nodeName) {
		return
	}

	state := getDtFailureNodeState()
	state.mu.Lock()
	defer state.mu.Unlock()

	if state.armed || state.recovered {
		return
	}
	state.convergedPrefixes[info.Prefix] = true
	for _, face := range info.DtValidFaces {
		state.candidateFaces[face] = true
	}
	if len(state.convergedPrefixes) < cfg.ExpectedPrefixes {
		return
	}

	faces := dtSortedFaceKeys(state.candidateFaces)
	if len(faces) <= 1 {
		state.recovered = true
		core.Log.Warn(s, "DT face failure skipped: not enough distinct valid faces",
			"node", nodeName,
			"prefixes", len(state.convergedPrefixes),
			"candidateFaces", faces)
		return
	}

	state.failedFace = faces[0]
	state.startTime = now.Add(cfg.StartOffset)
	state.endTime = now.Add(cfg.EndOffset)
	state.armed = true
	core.Log.Info(s, "DT face failure armed",
		"node", nodeName,
		"failedFace", state.failedFace,
		"startIn", cfg.StartOffset,
		"endIn", cfg.EndOffset,
		"prefixes", len(state.convergedPrefixes),
		"candidateFaces", faces)
}

func (s *Multipath) dtFailureSnapshot(now time.Time) dtFailureSnapshot {
	cfg := getDtFailureConfig()
	nodeName := s.dtNodeNameNormalized()
	if !cfg.Enabled || !dtFailureEligibleNode(nodeName) {
		return dtFailureSnapshot{}
	}

	state := getDtFailureNodeState()
	state.mu.Lock()
	defer state.mu.Unlock()

	snapshot := dtFailureSnapshot{
		Enabled:    state.armed && state.failedFace != 0,
		FailedFace: state.failedFace,
	}
	if !snapshot.Enabled {
		return snapshot
	}

	if state.active && !now.Before(state.endTime) {
		state.active = false
		state.recovered = true
		core.Log.Info(s, "DT face failure recovered",
			"node", nodeName,
			"face", state.failedFace,
			"at", now)
	}

	if !state.active && !state.recovered && !now.Before(state.startTime) && now.Before(state.endTime) {
		state.active = true
		state.activationEpoch++
		core.Log.Warn(s, "DT face failure started",
			"node", nodeName,
			"face", state.failedFace,
			"windowEnd", state.endTime)
	}

	snapshot.Active = state.active
	snapshot.ActivationEpoch = state.activationEpoch
	return snapshot
}

func (s *Multipath) dtBlockedFace(now time.Time, upstreamFace uint64) bool {
	snapshot := s.dtFailureSnapshot(now)
	return snapshot.Active && snapshot.FailedFace == upstreamFace
}

func (s *Multipath) blockedFaceFallbackCandidates(prefix string, blockedFace uint64) []uint64 {
	info := s.getPathDiscoveryInfo(prefix)
	info.mu.Lock()
	defer info.mu.Unlock()
	return dtFilterFacesWithoutFailure(info.DtValidFaces, blockedFace)
}

func (s *Multipath) reassignLocalDtInterestsFromFailedFace(blockedFace uint64) int {
	if blockedFace == 0 {
		return 0
	}

	reassigned := 0
	for prefix, queueState := range s.dtQueuesByPrefix {
		if queueState == nil || len(queueState.queue) == 0 {
			continue
		}
		for _, item := range queueState.queue {
			if item == nil || item.OwnerThreadID != s.threadID || item.AssignedFace != blockedFace {
				continue
			}

			survivors := dtFilterFacesWithoutFailure(item.CandidateFaces, blockedFace)
			if len(survivors) == 0 {
				survivors = s.blockedFaceFallbackCandidates(prefix, blockedFace)
			}
			if len(survivors) == 0 {
				continue
			}

			newFace := s.selectDtAssignedFaceWRR(prefix, survivors)
			if newFace == 0 {
				continue
			}

			noteDtThreadDemand(blockedFace, s.threadID, -1)
			noteDtThreadDemand(newFace, s.threadID, 1)
			s.noteLocalPrefixEligibleForFace(newFace, prefix)
			item.CandidateFaces = append([]uint64(nil), survivors...)
			item.AssignedFace = newFace
			reassigned++
		}
	}

	return reassigned
}

func (s *Multipath) syncDtFailureState(now time.Time) dtFailureSnapshot {
	snapshot := s.dtFailureSnapshot(now)
	if snapshot.Active && snapshot.ActivationEpoch != 0 && snapshot.ActivationEpoch != s.dtFailureAppliedEpoch {
		reassigned := s.reassignLocalDtInterestsFromFailedFace(snapshot.FailedFace)
		s.dtFailureAppliedEpoch = snapshot.ActivationEpoch
		core.Log.Info(s, "DT face failure local reassignment complete",
			"thread", s.threadID,
			"face", snapshot.FailedFace,
			"reassigned", reassigned,
			"activationEpoch", snapshot.ActivationEpoch)
	}
	return snapshot
}

func clearDtFaceBlockedStateLocked(control *dtFaceControlState, now time.Time) {
	control.tokens = 0
	control.lastRefill = now
	control.livenessForcedPackets = 0
	control.starvationZeroSendRounds = 0
	control.roundMintedTokens = 0
	for i := range control.budgetByThread {
		control.budgetByThread[i] = 0
	}
	for i := range control.carryByThread {
		control.carryByThread[i] = 0
	}
}

func (s *Multipath) getDtFaceCsvSinkLocked(control *dtFaceControlState, upstreamFace uint64) *dtFaceCsvSink {
	nodeName := s.dtNodeNameNormalized()
	if nodeName == "" {
		return nil
	}
	if control.csvSink != nil {
		return control.csvSink
	}
	control.csvSink = &dtFaceCsvSink{
		path: filepath.Join(dtFaceStatsDir(), nodeName+"_face"+strconv.FormatUint(upstreamFace, 10)+".csv"),
	}
	return control.csvSink
}

func dtCsvFloat(v float64) string {
	return strconv.FormatFloat(v, 'f', 6, 64)
}

type dtBwTrend int

const (
	dtBwTrendUnknown dtBwTrend = iota
	dtBwTrendIncreasing
	dtBwTrendDecreasing
	dtBwTrendFluctuating
)

func dtBwObservationSpacing(period time.Duration) time.Duration {
	if period <= 0 {
		period = dtStaticControlPeriod
	}
	spacing := period / 3
	if spacing <= 0 {
		spacing = time.Millisecond
	}
	return spacing
}

func dtAverageFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, value := range values {
		sum += value
	}
	return sum / float64(len(values))
}

func dtClassifyBwTrend(observations []float64) dtBwTrend {
	if len(observations) < 2 {
		return dtBwTrendUnknown
	}

	increasing := true
	decreasing := true
	for i := 1; i < len(observations); i++ {
		if observations[i] <= observations[i-1] {
			increasing = false
		}
		if observations[i] >= observations[i-1] {
			decreasing = false
		}
	}
	switch {
	case increasing:
		return dtBwTrendIncreasing
	case decreasing:
		return dtBwTrendDecreasing
	default:
		return dtBwTrendFluctuating
	}
}

func noteDtFaceCsvStart(now time.Time) {
	dtFaceCsvStartMu.Lock()
	defer dtFaceCsvStartMu.Unlock()
	if dtFaceCsvStart.IsZero() {
		dtFaceCsvStart = now
	}
}

func (s *Multipath) writeDtFaceCsvRound(
	sink *dtFaceCsvSink,
	roundNow time.Time,
	controlState string,
	controlAction string,
	interestQ float64,
	interestSlope float64,
	dataQ float64,
	dataSlope float64,
	estimatedBWFace float64,
	bwSamplesFace int,
	measuredSendPps float64,
	prevRate float64,
	newRate float64,
) {
	if sink == nil {
		return
	}

	dtFaceCsvStartMu.Lock()
	if dtFaceCsvStart.IsZero() {
		dtFaceCsvStart = roundNow
	}
	elapsedMs := roundNow.Sub(dtFaceCsvStart).Seconds() * 1000.0
	dtFaceCsvStartMu.Unlock()

	sink.mu.Lock()
	defer sink.mu.Unlock()

	if sink.writer == nil {
		if err := os.MkdirAll(filepath.Dir(sink.path), 0o755); err != nil {
			core.Log.Warn(s, "Failed to create DT face CSV directory", "path", sink.path, "err", err)
			return
		}
		file, err := os.OpenFile(sink.path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			core.Log.Warn(s, "Failed to open DT face CSV", "path", sink.path, "err", err)
			return
		}
		sink.file = file
		sink.writer = csv.NewWriter(file)
	}

	if !sink.headerWritten {
		header := []string{
			"time_ms",
			"interest_q",
			"interest_slope",
			"data_q",
			"data_slope",
			"bw_est_mbps",
			"bw_samples",
			"target_prev_mbps",
			"send_actual_mbps",
			"target_new_mbps",
			"control_state",
			"control_action",
		}
		if err := sink.writer.Write(header); err != nil {
			core.Log.Warn(s, "Failed to write DT face CSV header", "path", sink.path, "err", err)
			return
		}
		sink.headerWritten = true
	}

	row := []string{
		dtCsvFloat(elapsedMs),
		dtCsvFloat(interestQ),
		dtCsvFloat(interestSlope),
		dtCsvFloat(dataQ),
		dtCsvFloat(dataSlope),
		dtCsvFloat(dtRatePpsToMbps(estimatedBWFace)),
		strconv.Itoa(bwSamplesFace),
		dtCsvFloat(dtRatePpsToMbps(prevRate)),
		dtCsvFloat(dtRatePpsToMbps(measuredSendPps)),
		dtCsvFloat(dtRatePpsToMbps(newRate)),
		controlState,
		controlAction,
	}
	if err := sink.writer.Write(row); err != nil {
		core.Log.Warn(s, "Failed to write DT face CSV row", "path", sink.path, "err", err)
		return
	}
	sink.writer.Flush()
	if err := sink.writer.Error(); err != nil {
		core.Log.Warn(s, "Failed to flush DT face CSV", "path", sink.path, "err", err)
	}
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
	now := time.Now()
	noteDtFaceCsvStart(now)
	failureSnapshot := s.syncDtFailureState(now)

	// Extract sequence number from interest name for sparse DT telemetry.
	interestName := packet.Name.String()
	seqNum := extractSeqNum(interestName)

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
	dtFaces := candidateFaces
	dtFaceRateControlEnabled := true
	if info.IsNodeConverged {
		if len(info.DtValidFaces) > 0 {
			dtFaces = append([]uint64(nil), info.DtValidFaces...)
		}
		dtFaceRateControlEnabled = info.DtFaceRateControlEnabled
	}
	if failureSnapshot.Active {
		dtFaces = dtFilterFacesWithoutFailure(dtFaces, failureSnapshot.FailedFace)
	}
	if len(dtFaces) == 0 {
		info.mu.Unlock()
		core.Log.Warn(s, "DT forwarding failed: no valid upstream face after PD/failure filtering",
			"name", interestName,
			"prefix", prefixKey,
			"blockedFace", failureSnapshot.FailedFace)
		return
	}

	if !dtFaceRateControlEnabled {
		directFace := dtFaces[0]
		info.mu.Unlock()
		s.SendInterest(packet, pitEntry, directFace, inFace)
		if seqNum >= 0 && seqNum%200 == 0 {
			core.Log.Debug(s, "DT single-path immediate forward",
				"name", interestName,
				"prefix", prefixKey,
				"face", directFace)
		}
		return
	}

	s.ensureDtControlActivated(prefixKey, dtFaces, now)
	readyFaces, unreadyFaces := splitDtShapingReadyFaces(dtFaces)

	// Pre-shaping bootstrap mode:
	// If no candidate face is ready for shaped scheduling yet, forward this Interest
	// immediately using per-prefix RR across all candidate faces.
	if len(readyFaces) == 0 {
		bootstrapFace := selectDtBootstrapFaceRRLocked(info, dtFaces)
		info.mu.Unlock()
		s.SendInterest(packet, pitEntry, bootstrapFace, inFace)
		if seqNum >= 0 && seqNum%200 == 0 {
			core.Log.Debug(s, "DT immediate bootstrap forward",
				"name", interestName,
				"prefix", prefixKey,
				"face", bootstrapFace,
				"candidateFaces", dtFaces)
		}
		return
	}

	// Partial bootstrap mode:
	// Start shaped scheduling as soon as any face is ready, but keep probing unready
	// faces occasionally so their RTT bootstrap can complete. This avoids one slow
	// face keeping the whole prefix in immediate-forwarding mode indefinitely.
	if len(unreadyFaces) > 0 && shouldProbeDtUnreadyFaceLocked(info) {
		bootstrapFace := selectDtBootstrapFaceRRLocked(info, unreadyFaces)
		info.mu.Unlock()
		s.SendInterest(packet, pitEntry, bootstrapFace, inFace)
		if seqNum >= 0 && seqNum%200 == 0 {
			core.Log.Debug(s, "DT partial bootstrap probe",
				"name", interestName,
				"prefix", prefixKey,
				"face", bootstrapFace,
				"readyFaces", readyFaces,
				"unreadyFaces", unreadyFaces)
		}
		return
	}

	assignedFace := s.selectDtAssignedFaceWRR(prefixKey, readyFaces)
	if assignedFace == 0 {
		info.mu.Unlock()
		core.Log.Warn(s, "DT assignment failed: no usable upstream face",
			"name", interestName,
			"prefix", prefixKey,
			"candidateFaces", dtFaces,
			"readyFaces", readyFaces,
			"unreadyFaces", unreadyFaces)
		return
	}

	item := &InterestQueueItem{
		Packet:         packet,
		PitEntry:       pitEntry,
		InFace:         inFace,
		OwnerThreadID:  s.threadID,
		CandidateFaces: readyFaces,
		AssignedFace:   assignedFace,
		EnqueueTime:    time.Now(),
	}
	localQueueLen, globalQueueLen := s.enqueueLocalDtInterest(prefixKey, item)
	if seqNum >= 0 && seqNum%200 == 0 {
		core.Log.Debug(s, "DT enqueue",
			"name", interestName,
			"prefix", prefixKey,
			"ownerThread", s.threadID,
			"candidateFaces", dtFaces,
			"readyFaces", readyFaces,
			"unreadyFaces", unreadyFaces,
			"assignedFace", assignedFace,
			"localQueueLen", localQueueLen,
			"globalPrefixQueueLen", globalQueueLen)
	}
	info.mu.Unlock()
}

func splitDtShapingReadyFaces(candidateFaces []uint64) ([]uint64, []uint64) {
	readyFaces := make([]uint64, 0, len(candidateFaces))
	unreadyFaces := make([]uint64, 0, len(candidateFaces))
	for _, face := range candidateFaces {
		control := getDtFaceControlState(face)
		control.mu.Lock()
		ready := control.activated && control.rate > 0
		control.mu.Unlock()
		if ready {
			readyFaces = append(readyFaces, face)
		} else {
			unreadyFaces = append(unreadyFaces, face)
		}
	}
	return readyFaces, unreadyFaces
}

// shouldProbeDtUnreadyFaceLocked limits unshaped bootstrap probes once at least
// one ready face can carry shaped traffic. THREAD SAFETY: caller must hold info.mu.
func shouldProbeDtUnreadyFaceLocked(info *PathDiscoveryInfo) bool {
	info.DtBootstrapProbeCounter++
	return info.DtBootstrapProbeCounter%dtPartialBootstrapProbeEvery == 0
}

// selectDtBootstrapFaceRRLocked picks one candidate upstream face in RR order.
// THREAD SAFETY: caller must hold info.mu.
func selectDtBootstrapFaceRRLocked(info *PathDiscoveryInfo, candidateFaces []uint64) uint64 {
	if len(candidateFaces) == 0 {
		return 0
	}

	if info.DtBootstrapFaceRR < 0 || info.DtBootstrapFaceRR >= len(candidateFaces) {
		info.DtBootstrapFaceRR = 0
	}
	face := candidateFaces[info.DtBootstrapFaceRR]
	info.DtBootstrapFaceRR = (info.DtBootstrapFaceRR + 1) % len(candidateFaces)
	return face
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
	dtPacket, err := datapacket.DeserializeData(originalContent)
	if err != nil {
		// Log warning but allow forwarding (robustness)
		core.Log.Warn(s, "Failed to deserialize DataPacket", "err", err, "name", packet.Name)
	} else {
		interestQsf = dtPacket.InterestQsf
		dataQsf = dtPacket.DataQsf
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
	singlePathDtBypass := info.IsNodeConverged && !info.DtFaceRateControlEnabled
	if !singlePathDtBypass {
		s.updateDtUpstreamMeasurements(info, prefix, inFace, now, len(originalContent), interestQsf, dataQsf)
		s.maybeInitDtControlPeriodFromRtt(info, pitEntry, inFace, now)
	}
	info.mu.Unlock()
	interestQueueQsf := interestQsf
	if !singlePathDtBypass {
		interestQueuePackets := float64(globalDtPrefixQueueLen(prefix))
		interestQueueCapacity := dtGlobalInterestQueueCapacityPackets()
		interestQueueQsf = normalizeDtQueueSignal(interestQueuePackets, interestQueueCapacity)
	}

	// Disabled to keep DT logs focused on scheduler analysis.
	// if seqNum >= 0 && seqNum%100 == 0 {
	// 	core.Log.Debug(s, "DT metadata received",
	// 		"name", dataName,
	// 		"seq", seqNum,
	// 		"inFace", inFace,
	// 		"metadataDecoded", metadataDecoded,
	// 		"rxInterestQsf", interestQsf,
	// 		"rxDataQsf", dataQsf,
	// 		"windowAvgInterestQsf", upstreamInterestQsfAvg,
	// 		"windowAvgDataQsf", upstreamDataQsfAvg,
	// 		"windowSlopeInterestQsf", upstreamInterestQsfSlope,
	// 		"windowSlopeDataQsf", upstreamDataQsfSlope,
	// 		"estimatedBWPpsFace", upstreamEstimatedBW,
	// 		"measuredBWPpsFace", upstreamMeasuredBW,
	// 		"bwSamplesFace", upstreamBWSamples,
	// 		"bwUpdateRule", upstreamBWRule,
	// 		"windowSamples", upstreamWindowSamples)
	// }
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
		// Update outgoing DT metadata with this forwarder's queue signals:
		// - InterestQsf: normalized global per-prefix DT Interest queue occupancy
		//   across all forwarding-thread shards for this node
		// - DataQsf: downstream face queue pressure selected by dtDataQsfSource
		downstreamDataQsf := 0.0
		rawDownstreamDataQsf := 0.0
		downstreamDataQsfCapacity := 1.0
		transportQueueBytes := uint64(0)
		transportQueuePacketsApprox := 0.0
		linkQueuePackets := uint64(0)
		linkBacklogPackets := uint64(0)
		if outFace := dispatch.GetFace(faceID); outFace != nil {
			transportQueueBytes = outFace.GetSendQueueSize()
			linkQueuePackets = outFace.GetLinkSendQueueLen()
			_ = outFace.GetLinkBacklogBytes()
			linkBacklogPackets = outFace.GetLinkBacklogPackets()
			transportQueuePacketsApprox = float64(transportQueueBytes) / float64(dataPayloadSizeBytes)
			switch dtDataQsfSource {
			case "qdisc":
				rawDownstreamDataQsf = float64(linkBacklogPackets)
				downstreamDataQsfCapacity = dtQdiscQueueCapacityPackets
			case "link-service":
				rawDownstreamDataQsf = float64(linkQueuePackets)
				downstreamDataQsfCapacity = dtLinkServiceQueueCapacityPackets
			default:
				rawDownstreamDataQsf = transportQueuePacketsApprox
				downstreamDataQsfCapacity = dtInterestQueueCapacityPackets
			}
			downstreamDataQsf = normalizeDtQueueSignal(rawDownstreamDataQsf, downstreamDataQsfCapacity)
		} else {
			core.Log.Warn(s, "Downstream face missing while updating DT metadata", "faceid", faceID, "name", packet.Name)
		}

		// If payload cannot be decoded, keep current forwarding behavior.
		if dtPacket == nil {
			s.SendData(packet, pitEntry, faceID, inFace)
			continue
		}

		// Disabled to keep DT logs focused on scheduler analysis.
		// if seqNum >= 0 && seqNum%100 == 0 {
		// 	core.Log.Debug(s, "DT metadata attached",
		// 		"name", dataName,
		// 		"seq", seqNum,
		// 		"inFace", inFace,
		// 		"outFace", faceID,
		// 		"interestQueuePackets", interestQueuePackets,
		// 		"interestQsfCapacity", interestQueueCapacity,
		// 		"interestQsfAttached", interestQueueQsf,
		// 		"dataQsfSource", dtDataQsfSource,
		// 		"dataQsfRaw", rawDownstreamDataQsf,
		// 		"dataQsfCapacity", downstreamDataQsfCapacity,
		// 		"dataQsfAttached", downstreamDataQsf,
		// 		"transportQueueBytes", transportQueueBytes,
		// 		"transportQueuePacketsApprox", transportQueuePacketsApprox,
		// 		"linkSendQueuePackets", linkQueuePackets,
		// 		"qdiscBacklogBytes", linkBacklogBytes,
		// 		"qdiscBacklogPackets", linkBacklogPackets,
		// 		"qdiscBacklogQsf", linkBacklogQsf,
		// 		"payloadBytes", dataPayloadSizeBytes)
		// }

		faceDtPacket := *dtPacket
		faceDtPacket.InterestQsf = interestQueueQsf
		if singlePathDtBypass {
			faceDtPacket.DataQsf = math.Max(dataQsf, downstreamDataQsf)
		} else {
			faceDtPacket.DataQsf = downstreamDataQsf
		}

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

func getDtFaceBandwidthState(face uint64) *dtFaceBandwidthState {
	val, _ := dtFaceBandwidthByFace.LoadOrStore(face, &dtFaceBandwidthState{})
	return val.(*dtFaceBandwidthState)
}

func getDtGlobalPrefixQueueState(prefix string) *dtGlobalPrefixQueueState {
	val, _ := dtGlobalPrefixQueueByPrefix.LoadOrStore(prefix, &dtGlobalPrefixQueueState{})
	return val.(*dtGlobalPrefixQueueState)
}

func noteDtGlobalPrefixQueue(prefix string, delta int) int {
	if delta == 0 {
		return globalDtPrefixQueueLen(prefix)
	}
	state := getDtGlobalPrefixQueueState(prefix)
	next := state.length.Add(int64(delta))
	if next < 0 {
		state.length.Store(0)
		return 0
	}
	return int(next)
}

func globalDtPrefixQueueLen(prefix string) int {
	state := getDtGlobalPrefixQueueState(prefix)
	current := state.length.Load()
	if current < 0 {
		return 0
	}
	return int(current)
}

func (s *Multipath) getDtThreadFaceExecState(face uint64) *dtThreadFaceExecState {
	if state, ok := s.dtExecByFace[face]; ok {
		return state
	}
	state := &dtThreadFaceExecState{}
	s.dtExecByFace[face] = state
	return state
}

func (s *Multipath) getDtPrefixAssignState(prefix string) *dtPrefixFaceAssignState {
	state := s.dtAssignByPrefix[prefix]
	if state == nil {
		state = &dtPrefixFaceAssignState{
			credits:     make(map[uint64]float64),
			assignCount: make(map[uint64]uint64),
		}
		s.dtAssignByPrefix[prefix] = state
	}
	return state
}

func dtCurrentFaceWeight(face uint64) float64 {
	control := getDtFaceControlState(face)
	control.mu.Lock()
	rate := control.rate
	control.mu.Unlock()
	if rate <= 0 {
		rate = 1
	}
	return rate
}

// selectDtAssignedFaceWRR assigns one Interest to exactly one upstream face.
// It uses per-thread, per-prefix smooth weighted RR with current face rates as
// weights. This prevents multiple face schedulers from racing for the same IQ item.
func (s *Multipath) selectDtAssignedFaceWRR(prefix string, candidateFaces []uint64) uint64 {
	if len(candidateFaces) == 0 {
		return 0
	}

	state := s.getDtPrefixAssignState(prefix)
	var totalWeight float64
	var selected uint64
	bestCredit := math.Inf(-1)

	live := make(map[uint64]bool, len(candidateFaces))
	for _, face := range candidateFaces {
		live[face] = true
		weight := dtCurrentFaceWeight(face)
		totalWeight += weight
		state.credits[face] += weight
		if selected == 0 || state.credits[face] > bestCredit || (state.credits[face] == bestCredit && face < selected) {
			selected = face
			bestCredit = state.credits[face]
		}
	}
	for face := range state.credits {
		if !live[face] {
			delete(state.credits, face)
			delete(state.assignCount, face)
		}
	}
	if selected == 0 {
		return 0
	}
	state.credits[selected] -= totalWeight
	state.assignCount[selected]++
	state.logCount++

	if state.logCount%200 == 0 {
		core.Log.Debug(s, "DT assignment summary",
			"thread", s.threadID,
			"prefix", prefix,
			"candidateFaces", candidateFaces,
			"assignedFace", selected,
			"assignCounts", state.assignCount,
			"credits", state.credits)
	}
	return selected
}

func (s *Multipath) enqueueLocalDtInterest(prefix string, item *InterestQueueItem) (int, int) {
	state := s.dtQueuesByPrefix[prefix]
	if state == nil {
		state = &dtLocalPrefixQueue{prefix: prefix}
		s.dtQueuesByPrefix[prefix] = state
	}

	state.queue = append(state.queue, item)
	globalQueueLen := noteDtGlobalPrefixQueue(prefix, 1)
	if item.AssignedFace != 0 {
		s.noteLocalPrefixEligibleForFace(item.AssignedFace, prefix)
		noteDtThreadDemand(item.AssignedFace, s.threadID, 1)
	}
	return len(state.queue), globalQueueLen
}

func (s *Multipath) noteLocalPrefixEligibleForFace(face uint64, prefix string) {
	if !s.dtLocalFaceSeen[face] {
		s.dtLocalFaceSeen[face] = true
		s.dtLocalFaceOrder = append(s.dtLocalFaceOrder, face)
	}

	known := s.dtLocalFaceKnown[face]
	if known == nil {
		known = make(map[string]bool)
		s.dtLocalFaceKnown[face] = known
	}
	if known[prefix] {
		return
	}
	known[prefix] = true
	s.dtLocalFacePrefixes[face] = append(s.dtLocalFacePrefixes[face], prefix)
}

// localPendingInterestsForFace returns queued DT interests owned by this
// forwarding thread that are currently eligible to be sent on upstreamFace.
func (s *Multipath) localPendingInterestsForFace(upstreamFace uint64) int {
	control := getDtFaceControlState(upstreamFace)
	control.mu.Lock()
	defer control.mu.Unlock()
	ensureDtControlThreadSlotsLocked(control)
	if s.threadID >= len(control.demandByThread) {
		return 0
	}
	return control.demandByThread[s.threadID]
}

// dequeueDtInterestForFaceRR pops one local PIT-owned DT interest using RR across
// this forwarding thread's local prefix queues. It never touches another thread's PIT.
func (s *Multipath) dequeueDtInterestForFaceRR(upstreamFace uint64, exec *dtThreadFaceExecState) *InterestQueueItem {
	prefixes := s.dtLocalFacePrefixes[upstreamFace]
	if len(prefixes) == 0 {
		return nil
	}

	start := int(exec.prefixRR % uint64(len(prefixes)))
	for i := 0; i < len(prefixes); i++ {
		idx := (start + i) % len(prefixes)
		state := s.dtQueuesByPrefix[prefixes[idx]]
		if state == nil || len(state.queue) == 0 {
			continue
		}

		itemIdx := -1
		var item *InterestQueueItem
		for j, candidate := range state.queue {
			if candidate != nil && candidate.OwnerThreadID == s.threadID && candidate.AssignedFace == upstreamFace {
				itemIdx = j
				item = candidate
				break
			}
		}
		if itemIdx < 0 {
			continue
		}

		copy(state.queue[itemIdx:], state.queue[itemIdx+1:])
		state.queue[len(state.queue)-1] = nil
		state.queue = state.queue[:len(state.queue)-1]
		noteDtGlobalPrefixQueue(prefixes[idx], -1)
		if item.AssignedFace != 0 {
			noteDtThreadDemand(item.AssignedFace, s.threadID, -1)
		}
		exec.prefixRR = uint64(idx + 1)
		return item
	}

	return nil
}

func ensureDtControlThreadSlotsLocked(control *dtFaceControlState) {
	n := CfgNumThreads()
	if n <= 0 {
		n = 1
	}
	if len(control.demandByThread) < n {
		next := make([]int, n)
		copy(next, control.demandByThread)
		control.demandByThread = next
	}
	if len(control.budgetByThread) < n {
		next := make([]int, n)
		copy(next, control.budgetByThread)
		control.budgetByThread = next
	}
	if len(control.carryByThread) < n {
		next := make([]float64, n)
		copy(next, control.carryByThread)
		control.carryByThread = next
	}
	if len(control.roundGrantedByThread) < n {
		next := make([]uint64, n)
		copy(next, control.roundGrantedByThread)
		control.roundGrantedByThread = next
	}
	if len(control.roundUsedByThread) < n {
		next := make([]uint64, n)
		copy(next, control.roundUsedByThread)
		control.roundUsedByThread = next
	}
	if len(control.roundZeroBudgetByThread) < n {
		next := make([]uint64, n)
		copy(next, control.roundZeroBudgetByThread)
		control.roundZeroBudgetByThread = next
	}
	if len(control.roundEmptyBudgetByThread) < n {
		next := make([]uint64, n)
		copy(next, control.roundEmptyBudgetByThread)
		control.roundEmptyBudgetByThread = next
	}
	if len(control.roundSendFailByThread) < n {
		next := make([]uint64, n)
		copy(next, control.roundSendFailByThread)
		control.roundSendFailByThread = next
	}
	if len(control.roundPendingSumByThread) < n {
		next := make([]uint64, n)
		copy(next, control.roundPendingSumByThread)
		control.roundPendingSumByThread = next
	}
	if len(control.roundPendingMaxByThread) < n {
		next := make([]int, n)
		copy(next, control.roundPendingMaxByThread)
		control.roundPendingMaxByThread = next
	}
	if len(control.roundVisitsByThread) < n {
		next := make([]uint64, n)
		copy(next, control.roundVisitsByThread)
		control.roundVisitsByThread = next
	}
	if control.budgetRR >= n {
		control.budgetRR = 0
	}
}

func noteDtThreadDemand(face uint64, threadID int, delta int) {
	if threadID < 0 {
		return
	}
	control := getDtFaceControlState(face)
	control.mu.Lock()
	ensureDtControlThreadSlotsLocked(control)
	if threadID < len(control.demandByThread) {
		control.demandByThread[threadID] += delta
		if control.demandByThread[threadID] < 0 {
			control.demandByThread[threadID] = 0
		}
		if control.demandByThread[threadID] == 0 && threadID < len(control.budgetByThread) {
			control.budgetByThread[threadID] = 0
		}
		if control.demandByThread[threadID] == 0 && threadID < len(control.carryByThread) {
			control.carryByThread[threadID] = 0
		}
	}
	control.mu.Unlock()
}

func takeDtThreadBudget(face uint64, threadID int, maxPackets int) int {
	if threadID < 0 || maxPackets <= 0 {
		return 0
	}
	control := getDtFaceControlState(face)
	control.mu.Lock()
	defer control.mu.Unlock()
	ensureDtControlThreadSlotsLocked(control)
	if threadID >= len(control.budgetByThread) {
		return 0
	}
	grant := control.budgetByThread[threadID]
	if grant <= 0 {
		return 0
	}
	if grant > maxPackets {
		grant = maxPackets
	}
	control.budgetByThread[threadID] -= grant
	return grant
}

func refillDtFaceTokensLocked(control *dtFaceControlState, now time.Time) {
	if control.lastRefill.IsZero() {
		control.lastRefill = now
	}
	elapsed := now.Sub(control.lastRefill).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	minted := elapsed * control.rate
	control.tokens += minted
	control.roundMintedTokens += minted
	maxBurst := dtFaceTokenBurstCapPackets
	if control.tokens > maxBurst {
		control.tokens = maxBurst
	}
	control.lastRefill = now
}

func totalDtThreadBudgetLocked(control *dtFaceControlState) int {
	total := 0
	for _, budget := range control.budgetByThread {
		total += budget
	}
	return total
}

func clampDtRetainedBudgetLocked(control *dtFaceControlState) {
	if control.tokens > dtRetainedBudgetCapPackets {
		control.tokens = dtRetainedBudgetCapPackets
	}
}

func reclaimDtLegacyBudgetLocked(control *dtFaceControlState) int {
	leftover := 0
	for tid := range control.budgetByThread {
		leftover += control.budgetByThread[tid]
		control.budgetByThread[tid] = 0
	}
	if leftover > 0 {
		control.tokens += float64(leftover)
		clampDtRetainedBudgetLocked(control)
	}
	return leftover
}

func assignDtWeightedThreadBudgetLocked(control *dtFaceControlState, availablePackets int, nThreads int, start int) int {
	if availablePackets <= 0 || nThreads <= 0 {
		return 0
	}

	totalWeight := 0.0
	weights := make([]float64, nThreads)
	for i := 0; i < nThreads; i++ {
		tid := (start + i) % nThreads
		demand := control.demandByThread[tid]
		if demand <= 0 {
			continue
		}
		weight := dtThreadActiveBaseWeight + float64(demand)
		weights[tid] = weight
		totalWeight += weight
	}
	if totalWeight <= 0 {
		return 0
	}

	assignedPackets := 0
	remainingPackets := availablePackets
	for i := 0; i < nThreads; i++ {
		tid := (start + i) % nThreads
		weight := weights[tid]
		if weight <= 0 {
			continue
		}
		share := (float64(availablePackets) * (weight / totalWeight)) + control.carryByThread[tid]
		grant := int(math.Floor(share))
		control.carryByThread[tid] = share - float64(grant)
		if control.carryByThread[tid] > dtThreadQuotaCarryCapPackets {
			control.carryByThread[tid] = dtThreadQuotaCarryCapPackets
		}
		room := dtThreadQuotaCapPackets - control.budgetByThread[tid]
		if room <= 0 || grant <= 0 {
			continue
		}
		if grant > room {
			grant = room
		}
		control.budgetByThread[tid] += grant
		assignedPackets += grant
		remainingPackets -= grant
	}

	for remainingPackets > 0 {
		bestTid := -1
		bestCarry := -1.0
		for i := 0; i < nThreads; i++ {
			tid := (start + i) % nThreads
			if weights[tid] <= 0 {
				continue
			}
			if control.budgetByThread[tid] >= dtThreadQuotaCapPackets {
				continue
			}
			if control.carryByThread[tid] > bestCarry {
				bestCarry = control.carryByThread[tid]
				bestTid = tid
			}
		}
		if bestTid < 0 {
			break
		}
		control.budgetByThread[bestTid]++
		control.carryByThread[bestTid] = 0
		assignedPackets++
		remainingPackets--
	}

	return assignedPackets
}

func noteDtRoundSchedulerEvent(face uint64, threadID int, pending int, granted int, used int, zeroBudget bool, emptyBudget int, sendFail int) {
	if threadID < 0 {
		return
	}
	control := getDtFaceControlState(face)
	control.mu.Lock()
	defer control.mu.Unlock()
	ensureDtControlThreadSlotsLocked(control)
	if threadID >= len(control.roundVisitsByThread) {
		return
	}
	control.roundVisitsByThread[threadID]++
	if pending > 0 {
		control.roundPendingSumByThread[threadID] += uint64(pending)
		if pending > control.roundPendingMaxByThread[threadID] {
			control.roundPendingMaxByThread[threadID] = pending
		}
	}
	if granted > 0 {
		control.roundGrantedByThread[threadID] += uint64(granted)
	}
	if used > 0 {
		control.roundUsedByThread[threadID] += uint64(used)
	}
	if zeroBudget {
		control.roundZeroBudgetByThread[threadID]++
	}
	if emptyBudget > 0 {
		control.roundEmptyBudgetByThread[threadID] += uint64(emptyBudget)
	}
	if sendFail > 0 {
		control.roundSendFailByThread[threadID] += uint64(sendFail)
	}
}

func (s *Multipath) allocateDtFaceBudgetsLegacy(upstreamFace uint64, _ time.Time, control *dtFaceControlState, schedulerEpoch uint64) {
	reclaimedBudget := reclaimDtLegacyBudgetLocked(control)
	_ = schedulerEpoch
	grant := int(control.tokens)
	if grant > 0 {
		control.tokens -= float64(grant)
	}
	if grant < 1 && control.livenessForcedPackets > 0 {
		grant = 1
		control.livenessForcedPackets--
	}
	if grant <= 0 {
		return
	}
	totalDemand := 0
	activeThreads := 0
	for _, demand := range control.demandByThread {
		totalDemand += demand
		if demand > 0 {
			activeThreads++
		}
	}
	if activeThreads == 0 {
		control.tokens += float64(grant)
		clampDtRetainedBudgetLocked(control)
		return
	}

	nThreads := len(control.demandByThread)
	start := control.budgetRR
	assignedPackets := assignDtWeightedThreadBudgetLocked(control, grant, nThreads, start)
	remaining := grant - assignedPackets
	if remaining > 0 {
		control.tokens += float64(remaining)
		clampDtRetainedBudgetLocked(control)
	}
	control.budgetRR = (start + 1) % nThreads

	control.budgetRounds++
	if control.budgetRounds%dtControllerDebugRounds == 0 {
		totalBudget := totalDtThreadBudgetLocked(control)
		core.Log.Debug(s, "DT budget allocation",
			"face", upstreamFace,
			"grant", grant,
			"reclaimedBudget", reclaimedBudget,
			"assignedPackets", assignedPackets,
			"activeThreads", activeThreads,
			"totalDemand", totalDemand,
			"totalBudget", totalBudget,
			"rate", control.rate,
			"period", control.period,
			"tokens", dtLogFloat1(control.tokens))
	}
}

func (s *Multipath) allocateDtFaceBudgets(upstreamFace uint64, now time.Time, schedulerEpoch uint64) {
	control := getDtFaceControlState(upstreamFace)
	control.mu.Lock()
	defer control.mu.Unlock()
	if !control.activated || control.rate <= 0 {
		return
	}
	if s.dtBlockedFace(now, upstreamFace) {
		ensureDtControlThreadSlotsLocked(control)
		clearDtFaceBlockedStateLocked(control, now)
		return
	}

	ensureDtControlThreadSlotsLocked(control)
	totalDemand := 0
	for _, demand := range control.demandByThread {
		totalDemand += demand
	}
	if totalDemand <= 0 {
		reclaimDtLegacyBudgetLocked(control)
		if control.tokens > dtRetainedBudgetCapPackets {
			control.tokens = dtRetainedBudgetCapPackets
		}
		control.lastRefill = now
		return
	}
	refillDtFaceTokensLocked(control, now)
	s.allocateDtFaceBudgetsLegacy(upstreamFace, now, control, schedulerEpoch)
}

func noteDtFaceSentPackets(face uint64, sent int) {
	if sent <= 0 {
		return
	}
	control := getDtFaceControlState(face)
	control.mu.Lock()
	control.sentCounter += uint64(sent)
	control.mu.Unlock()
}

func (s *Multipath) drainLocalDtQueues(now time.Time) {
	if len(s.dtLocalFaceOrder) == 0 {
		return
	}

	visits := len(s.dtLocalFaceOrder)
	if visits > dtSchedulerMaxFacesPerTick {
		visits = dtSchedulerMaxFacesPerTick
	}
	for i := 0; i < visits; i++ {
		idx := int(s.dtLocalFaceRR % uint64(len(s.dtLocalFaceOrder)))
		s.dtLocalFaceRR++
		face := s.dtLocalFaceOrder[idx]
		s.drainDtInterestForFace(now, face)
	}
}

func (s *Multipath) logDtConsumeAttempt(now time.Time, upstreamFace uint64, pendingBefore int, pendingAfter int, budget int, used int, emptyBudget int, sendFail int, stopReason string) {
	if !dtSchedulerConsumeDebug {
		return
	}
	if budget <= used {
		return
	}
	if stopReason == "empty-dequeue" && pendingAfter == 0 {
		return
	}
	core.Log.Warn(s, "DT consume tick underused budget",
		"thread", s.threadID,
		"face", upstreamFace,
		"pendingBefore", pendingBefore,
		"pendingAfter", pendingAfter,
		"budgetGranted", budget,
		"usedPackets", used,
		"unusedBudgetPackets", budget-used,
		"emptyBudgetPackets", emptyBudget,
		"sendFailPackets", sendFail,
		"stopReason", stopReason,
		"now", now)
}

func (s *Multipath) drainDtInterestForFace(now time.Time, upstreamFace uint64) {
	if s.dtBlockedFace(now, upstreamFace) {
		return
	}
	pending := s.localPendingInterestsForFace(upstreamFace)
	if pending == 0 {
		return
	}

	exec := s.getDtThreadFaceExecState(upstreamFace)
	exec.tickCount++

	budget := takeDtThreadBudget(upstreamFace, s.threadID, dtSchedulerMaxSendPerTick)
	if budget <= 0 {
		exec.tokenMisses++
		noteDtRoundSchedulerEvent(upstreamFace, s.threadID, pending, 0, 0, true, 0, 0)
		s.logDtSchedulerSummary(now, upstreamFace, exec, pending)
		return
	}

	used := 0
	sendFail := 0
	emptyBudget := 0
	stopReason := "budget-exhausted"
	for sent := 0; sent < budget; sent++ {
		item := s.dequeueDtInterestForFaceRR(upstreamFace, exec)
		if item == nil {
			exec.emptyDequeues++
			emptyBudget = budget - sent
			stopReason = "empty-dequeue"
			break
		}
		if s.SendInterest(item.Packet, item.PitEntry, upstreamFace, item.InFace) {
			exec.sentShaped++
			used++
		} else {
			sendFail++
			stopReason = "send-fail"
		}
		if !item.EnqueueTime.IsZero() {
			age := now.Sub(item.EnqueueTime)
			if age < 0 {
				age = 0
			}
			exec.sendAgeTotal += age
			if age > exec.sendAgeMax {
				exec.sendAgeMax = age
			}
			if age >= 100*time.Millisecond {
				exec.staleSends++
			}
		}
	}
	noteDtFaceSentPackets(upstreamFace, used)
	noteDtRoundSchedulerEvent(upstreamFace, s.threadID, pending, budget, used, false, emptyBudget, sendFail)
	pendingAfter := s.localPendingInterestsForFace(upstreamFace)
	s.logDtConsumeAttempt(now, upstreamFace, pending, pendingAfter, budget, used, emptyBudget, sendFail, stopReason)
	s.logDtSchedulerSummary(now, upstreamFace, exec, pending)
}

func (s *Multipath) logDtSchedulerSummary(now time.Time, upstreamFace uint64, exec *dtThreadFaceExecState, pending int) {
	if exec.tickCount%dtSchedulerDebugEveryTicks != 0 {
		return
	}

	control := getDtFaceControlState(upstreamFace)
	control.mu.Lock()
	currentRate := control.rate
	currentPeriod := control.period
	currentBudget := 0
	currentCarry := 0.0
	currentDemand := 0
	if s.threadID < len(control.budgetByThread) {
		currentBudget = control.budgetByThread[s.threadID]
	}
	if s.threadID < len(control.carryByThread) {
		currentCarry = control.carryByThread[s.threadID]
	}
	if s.threadID < len(control.demandByThread) {
		currentDemand = control.demandByThread[s.threadID]
	}
	control.mu.Unlock()

	totalSent := exec.sentBootstrap + exec.sentShaped
	ageAvgMs := 0.0
	if totalSent > 0 {
		ageAvgMs = float64(exec.sendAgeTotal.Microseconds()) / 1000.0 / float64(totalSent)
	}
	core.Log.Debug(s, "DT local scheduler",
		"thread", s.threadID,
		"face", upstreamFace,
		"rate", dtLogFloat1(currentRate),
		"period", currentPeriod,
		"pendingLocal", pending,
		"demandThread", currentDemand,
		"budgetThread", currentBudget,
		"carryThread", dtLogFloat1(currentCarry),
		"sentShaped", exec.sentShaped,
		"emptyDequeues", exec.emptyDequeues,
		"tokenMisses", exec.tokenMisses,
		"sendAgeAvgMs", dtLogFloat1(ageAvgMs),
		"sendAgeMaxMs", dtLogFloat1(float64(exec.sendAgeMax.Microseconds())/1000.0),
		"staleSends", exec.staleSends,
		"now", now)
	exec.sentBootstrap = 0
	exec.sentShaped = 0
	exec.emptyDequeues = 0
	exec.tokenMisses = 0
	exec.sendAgeTotal = 0
	exec.sendAgeMax = 0
	exec.staleSends = 0
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
		SampleCount:   sampleCount,
		LastUpdated:   sampleTime,
	}
	state.mu.Unlock()
}

func (s *Multipath) getDtFaceInterestSignal(face uint64, prefix string) (dtPrefixInterestSignal, bool) {
	state := getDtFaceInterestSignalState(face)
	state.mu.RLock()
	defer state.mu.RUnlock()
	signal, ok := state.byPref[prefix]
	return signal, ok
}

// getAveragedDtFaceQueueSignal returns face-level queue signals across prefixes.
// Queue-related signals are weighted averages across live prefix windows.
func (s *Multipath) getAveragedDtFaceQueueSignal(face uint64, now time.Time) (float64, float64, float64, float64, int) {
	state := getDtFaceInterestSignalState(face)
	state.mu.Lock()
	defer state.mu.Unlock()

	if len(state.byPref) == 0 {
		return 0, 0, 0, 0, 0
	}

	var sumW float64
	var sumIQ float64
	var sumIQSlope float64
	var sumDQ float64
	var sumDQSlope float64
	var countedPrefixes int
	for prefix, signal := range state.byPref {
		w := float64(signal.SampleCount)
		if w <= 0 {
			delete(state.byPref, prefix)
			continue
		}
		sumW += w
		sumIQ += w * signal.InterestQ
		sumIQSlope += w * signal.InterestSlope
		sumDQ += w * signal.DataQ
		sumDQSlope += w * signal.DataSlope
		countedPrefixes++
	}

	if countedPrefixes <= 0 || sumW <= 0 {
		return 0, 0, 0, 0, countedPrefixes
	}

	return sumIQ / sumW, sumIQSlope / sumW, sumDQ / sumW, sumDQSlope / sumW, countedPrefixes
}

// getAveragedDtFaceSignal combines per-face queue signals with the per-face
// bandwidth estimator. Bandwidth is never summed across prefixes.
func (s *Multipath) getAveragedDtFaceSignal(face uint64) (float64, float64, float64, float64, float64, int) {
	now := time.Now()
	interestQ, interestSlope, dataQ, dataSlope, prefixes := s.getAveragedDtFaceQueueSignal(face, now)
	bw, _, _, _ := s.getDtFaceBandwidthEstimate(face, now)
	return interestQ, interestSlope, dataQ, dataSlope, bw, prefixes
}

func noteDtFaceBandwidthSample(upstreamFace uint64, now time.Time) (float64, int) {
	state := getDtFaceBandwidthState(upstreamFace)
	state.mu.Lock()
	defer state.mu.Unlock()

	state.samples = append(state.samples, now)
	state.samples = trimDtFaceBandwidthSamples(state.samples, now)
	sampleCount := len(state.samples)
	measuredPPS := 0.0
	if sampleCount > 0 && dtThroughputWindowDur.Seconds() > 0 {
		measuredPPS = float64(sampleCount) / dtThroughputWindowDur.Seconds()
	}
	state.lastMeasuredPps = measuredPPS
	return measuredPPS, sampleCount
}

func applyDtFaceBandwidthEstimateLocked(state *dtFaceBandwidthState, measuredPPS float64, queuePressureQsf float64) string {
	prevBW := state.estimatedPps
	updateRule := "retain"
	qthLow := dtRateControlParams.MaxFwdQueueSize * dtRateControlParams.FwdQueueAlpha
	latest := state.observations[len(state.observations)-1]
	avgRecent := dtAverageFloat64(state.observations)
	trend := dtClassifyBwTrend(state.observations)
	triggerUp := measuredPPS > prevBW
	triggerPressure := queuePressureQsf > qthLow
	candidate := prevBW
	alpha := 0.0

	switch {
	case triggerUp && trend == dtBwTrendIncreasing:
		candidate = avgRecent
		alpha = dtBwAlphaUp
		updateRule = "throughput-high-monotonic"
	case triggerUp:
		candidate = latest
		alpha = dtBwAlphaSmall
		updateRule = "throughput-high-fluctuating"
	case triggerPressure && trend == dtBwTrendDecreasing:
		candidate = latest
		alpha = dtBwAlphaDown
		updateRule = "queue-pressure-monotonic"
	case triggerPressure:
		candidate = latest
		alpha = dtBwAlphaSmall
		updateRule = "queue-pressure-fluctuating"
	}

	if alpha > 0 {
		state.estimatedPps = prevBW + alpha*(candidate-prevBW)
		if state.estimatedPps >= dtFaceInitRate {
			state.capEligible = true
		}
	}
	state.lastUpdateRule = updateRule
	state.lastMeasuredPps = measuredPPS
	return updateRule
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
	if dtUseStaticControlPeriod {
		return true, dtStaticControlPeriod, 0
	}
	state := getDtRttBootstrapState(face)
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.ready, state.period, state.count
}

func recordDtRttBootstrapSample(face uint64, rtt time.Duration) (bool, time.Duration, int) {
	if dtUseStaticControlPeriod {
		return true, dtStaticControlPeriod, 0
	}
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
// It gives each face a small init rate, but leaves the periodic rate-control loop
// dormant until both control-period bootstrap and BW bootstrap are ready.
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
			control.rate = dtFaceInitRate
			control.action = DtActionHoldDraining
			control.lastRefill = now
			control.nextRun = time.Time{}
			core.Log.Debug(s, "DT face scheduler activated",
				"prefix", prefix,
				"face", face,
				"initRatePps", dtLogFloat1(dtFaceInitRate),
				"initRateMbps", dtLogFloat1(dtRatePpsToMbps(dtFaceInitRate)))
		}
		control.mu.Unlock()
	}
}

func (s *Multipath) activateDtFaceRateControl(prefix string, upstreamFace uint64, now time.Time, rttSamples int) {
	control := getDtFaceControlState(upstreamFace)
	control.mu.Lock()
	if !control.activated || control.rateControlActive || control.period <= 0 {
		control.mu.Unlock()
		return
	}
	period := control.period
	control.rateControlActive = true
	control.nextRun = now.Add(period)
	control.mu.Unlock()

	core.Log.Debug(s, "DT face rate-control activated",
		"prefix", prefix,
		"face", upstreamFace,
		"period", period,
		"rttSamples", rttSamples,
		"staticControlPeriod", dtUseStaticControlPeriod,
		"periodScale", dtControlPeriodRttScale)
}

// maybeInitDtControlPeriodFromRtt records RTT sample and stores the per-face
// control period when bootstrap is ready. The periodic rate-control loop only
// starts after both period bootstrap and BW bootstrap are ready.
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
	if control.rateControlActive {
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
	control.mu.Unlock()

	bwState := getDtFaceBandwidthState(upstreamFace)
	bwState.mu.Lock()
	bwReady := bwState.bootstrapReady
	bwState.mu.Unlock()
	if bwReady {
		s.activateDtFaceRateControl(info.Prefix, upstreamFace, now, samples)
	}
}

func (s *Multipath) runDtControllerTick(now time.Time, schedulerEpoch uint64) {
	dtFaceControlByFace.Range(func(key, value any) bool {
		face, ok := key.(uint64)
		if !ok {
			return true
		}

		control, ok := value.(*dtFaceControlState)
		if !ok || control == nil {
			return true
		}
		if s.dtBlockedFace(now, face) {
			control.mu.Lock()
			ensureDtControlThreadSlotsLocked(control)
			clearDtFaceBlockedStateLocked(control, now)
			control.mu.Unlock()
			return true
		}

		shouldRun := false
		control.mu.Lock()
		if control.activated && control.rateControlActive && control.period > 0 {
			if control.nextRun.IsZero() || !now.Before(control.nextRun) {
				control.nextRun = now.Add(control.period)
				shouldRun = true
			}
		}
		control.mu.Unlock()

		if shouldRun {
			s.faceSendRateEstimationCore(face)
		}
		s.allocateDtFaceBudgets(face, now, schedulerEpoch)
		return true
	})
}

func (s *Multipath) runDtFastSchedulerEpochIfDue(now time.Time) {
	dtFastSchedulerMu.Lock()
	defer dtFastSchedulerMu.Unlock()

	if !dtFastSchedulerNext.IsZero() && now.Before(dtFastSchedulerNext) {
		return
	}
	dtFastSchedulerEpoch++
	dtFastSchedulerNext = now.Add(strategyTickInterval)
	s.runDtControllerTick(now, dtFastSchedulerEpoch)
}

// OnTick drains this forwarding thread's local, PIT-owned DT queues.
// One thread first advances the shared DT fast scheduler epoch, which follows
// the forwarding thread strategy ticker cadence.
func (s *Multipath) OnTick(_ time.Time) {
	actualNow := time.Now()
	s.syncDtFailureState(actualNow)
	s.runDtFastSchedulerEpochIfDue(actualNow)
	s.drainLocalDtQueues(actualNow)
}

func initDtEstimatorTunables() {
	dtEstimatorTunableOnce.Do(func() {
		currentPktSize := datapacket.NewDataPacket().GetSize()
		if currentPktSize > 0 {
			dtDataPacketSizeBytes = currentPktSize
		}
		dtFaceInitRate = math.Max(1.0, dtRateMbpsToPps(targetDtFaceInitMbps))

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

func trimDtFaceBandwidthSamples(samples []time.Time, now time.Time) []time.Time {
	cutoff := now.Add(-dtThroughputWindowDur)
	validIdx := len(samples)
	for i, sampleTime := range samples {
		if !sampleTime.Before(cutoff) {
			validIdx = i
			break
		}
	}
	samples = samples[validIdx:]
	if len(samples) > dtMaxSamplesPerFace {
		samples = samples[len(samples)-dtMaxSamplesPerFace:]
	}
	return samples
}

func trimDtUpstreamQsfSamples(samples []DtUpstreamSample, now time.Time) []DtUpstreamSample {
	cutoff := now.Add(-dtThroughputWindowDur)
	validIdx := len(samples)
	for i, sample := range samples {
		if !sample.Timestamp.Before(cutoff) {
			validIdx = i
			break
		}
	}
	samples = samples[validIdx:]
	if len(samples) > dtQsfSampleLimit {
		samples = samples[len(samples)-dtQsfSampleLimit:]
	}
	return samples
}

func (s *Multipath) updateDtFaceBandwidthEstimate(upstreamFace uint64, now time.Time, queuePressureQsf float64) (float64, float64, int, string) {
	state := getDtFaceBandwidthState(upstreamFace)
	control := getDtFaceControlState(upstreamFace)
	control.mu.Lock()
	controlPeriod := control.period
	control.mu.Unlock()

	bootstrapJustReady := false
	state.mu.Lock()
	state.samples = trimDtFaceBandwidthSamples(state.samples, now)
	sampleCount := len(state.samples)
	measuredPPS := 0.0
	if sampleCount > 0 && dtThroughputWindowDur.Seconds() > 0 {
		measuredPPS = float64(sampleCount) / dtThroughputWindowDur.Seconds()
	}
	state.lastMeasuredPps = measuredPPS
	if sampleCount < dtBwSampleMin {
		if sampleCount == 0 {
			state.lastUpdateRule = "no-sample-retain"
		} else if state.estimatedPps > 0 {
			state.lastUpdateRule = "insufficient-sample-retain"
		} else {
			state.lastUpdateRule = "insufficient-sample-skip"
		}
		state.lastUpdated = now
		estimated := state.estimatedPps
		measured := state.lastMeasuredPps
		rule := state.lastUpdateRule
		state.mu.Unlock()
		return estimated, measured, sampleCount, rule
	}

	spacing := dtBwObservationSpacing(controlPeriod)
	if !state.lastObsTime.IsZero() && now.Sub(state.lastObsTime) < spacing {
		estimated := state.estimatedPps
		measured := state.lastMeasuredPps
		rule := "cadence-wait"
		state.lastUpdateRule = rule
		state.mu.Unlock()
		return estimated, measured, sampleCount, rule
	}

	state.lastObsTime = now
	state.observations = append(state.observations, measuredPPS)
	if len(state.observations) > dtBwObsLimit {
		state.observations = state.observations[len(state.observations)-dtBwObsLimit:]
	}

	if !state.bootstrapReady {
		if len(state.observations) < dtBwObsLimit {
			state.lastUpdated = now
			state.lastUpdateRule = "bootstrap-collecting"
			estimated := state.estimatedPps
			measured := state.lastMeasuredPps
			rule := state.lastUpdateRule
			state.mu.Unlock()
			return estimated, measured, sampleCount, rule
		}

		state.estimatedPps = dtAverageFloat64(state.observations)
		state.bootstrapReady = true
		state.lastUpdated = now
		state.lastUpdateRule = "bootstrap-ready"
		bootstrapJustReady = true
		estimated := state.estimatedPps
		measured := state.lastMeasuredPps
		rule := state.lastUpdateRule
		state.mu.Unlock()
		if bootstrapJustReady {
			core.Log.Debug(s, "DT face BW bootstrap ready",
				"face", upstreamFace,
				"estimatedBW", dtLogFloat1(estimated),
				"observations", dtBwObsLimit,
				"observationSpacing", spacing)
		}
		return estimated, measured, sampleCount, rule
	}

	updateRule := applyDtFaceBandwidthEstimateLocked(state, measuredPPS, queuePressureQsf)
	state.lastUpdated = now
	estimated := state.estimatedPps
	measured := state.lastMeasuredPps
	state.mu.Unlock()
	return estimated, measured, sampleCount, updateRule
}

func (s *Multipath) getDtFaceBandwidthEstimate(upstreamFace uint64, now time.Time) (float64, float64, int, string) {
	state := getDtFaceBandwidthState(upstreamFace)
	state.mu.Lock()
	defer state.mu.Unlock()

	state.samples = trimDtFaceBandwidthSamples(state.samples, now)
	sampleCount := len(state.samples)
	measuredPPS := 0.0
	if sampleCount > 0 && dtThroughputWindowDur.Seconds() > 0 {
		measuredPPS = float64(sampleCount) / dtThroughputWindowDur.Seconds()
	}
	if sampleCount < dtBwSampleMin {
		if sampleCount == 0 && state.estimatedPps > 0 && now.Sub(state.lastNoSampleWarn) >= dtBandwidthNoSampleWarnInterval {
			state.lastNoSampleWarn = now
			core.Log.Warn(s, "DT BW estimate retained without fresh samples",
				"face", upstreamFace,
				"retainedBWPps", state.estimatedPps,
				"lastMeasuredBWPps", state.lastMeasuredPps,
				"lastUpdateRule", state.lastUpdateRule,
				"staleFor", now.Sub(state.lastUpdated))
		}
		if state.estimatedPps > 0 {
			return state.estimatedPps, state.lastMeasuredPps, sampleCount, state.lastUpdateRule
		}
		return 0, 0, sampleCount, "insufficient-sample-skip"
	}
	if sampleCount > 0 {
		state.lastMeasuredPps = measuredPPS
		return state.estimatedPps, measuredPPS, sampleCount, state.lastUpdateRule
	}
	if state.estimatedPps > 0 && now.Sub(state.lastNoSampleWarn) >= dtBandwidthNoSampleWarnInterval {
		state.lastNoSampleWarn = now
		core.Log.Warn(s, "DT BW estimate retained without fresh samples",
			"face", upstreamFace,
			"retainedBWPps", state.estimatedPps,
			"lastMeasuredBWPps", state.lastMeasuredPps,
			"lastUpdateRule", state.lastUpdateRule,
			"staleFor", now.Sub(state.lastUpdated))
	}
	return state.estimatedPps, 0, 0, "no-sample-retain"
}

// updateDtUpstreamMeasurements maintains DT per-upstream sliding-window measurements.
// It updates:
//  1. per-face bandwidth estimate in packets/sec
//  2. per-prefix/per-upstream average InterestQsf/DataQsf metadata
//  3. per-prefix/per-upstream InterestQsf/DataQsf trend slopes (units: qsf per second), computed via
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
) (float64, float64, int, string) {
	if dtUseQsfBiasRemoval && !info.UpstreamInterestQsfBiasReady[upstreamFace] {
		info.UpstreamInterestQsfBias[upstreamFace] = interestQsf
		info.UpstreamInterestQsfBiasReady[upstreamFace] = true
		core.Log.Debug(s, "DT upstream interest QSF bias initialized",
			"prefix", prefix,
			"face", upstreamFace,
			"interestQsfBias", dtLogFloat1(interestQsf))
	}
	interestQsf = correctedDtQsf(interestQsf, info.UpstreamInterestQsfBias[upstreamFace])

	control := getDtFaceControlState(upstreamFace)
	logDataBias := false
	dataBias := 0.0
	control.mu.Lock()
	if dtUseQsfBiasRemoval && !control.dataQsfBiasReady {
		control.dataQsfBias = dataQsf
		control.dataQsfBiasReady = true
		logDataBias = true
		dataBias = dataQsf
	}
	currentDataBias := control.dataQsfBias
	control.mu.Unlock()
	if logDataBias {
		core.Log.Debug(s, "DT upstream data QSF bias initialized",
			"face", upstreamFace,
			"dataQsfBias", dtLogFloat1(dataBias))
	}
	dataQsf = correctedDtQsf(dataQsf, currentDataBias)

	samples := info.UpstreamSamples[upstreamFace]
	samples = append(samples, DtUpstreamSample{
		Timestamp:   now,
		SizeBytes:   payloadSize,
		InterestQsf: interestQsf,
		DataQsf:     dataQsf,
	})
	samples = trimDtUpstreamQsfSamples(samples, now)
	info.UpstreamSamples[upstreamFace] = samples

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
		len(samples),
		now,
	)
	noteDtFaceBandwidthSample(upstreamFace, now)
	return s.updateDtFaceBandwidthEstimate(upstreamFace, now, math.Max(interestQsf, dataQsf))
}

// faceSendRateEstimationCore performs one per-upstream-face control round.
// All input signals and control state are aggregated/stored at face level.
func (s *Multipath) faceSendRateEstimationCore(upstreamFace uint64) {
	// Step 1: derive queue signal from face-level aggregates across prefixes.
	interestQ, interestSlope, dataQ, dataSlope, estimatedBWFace, prefixes := s.getAveragedDtFaceSignal(upstreamFace)
	signalReason := "fresh"
	controlState := "normal"
	livePrefixes := prefixes
	control := getDtFaceControlState(upstreamFace)
	control.mu.Lock()
	if prefixes <= 0 {
		if control.lastSignalReady {
			interestQ = control.lastInterestQ
			interestSlope = control.lastInterestSlope
			dataQ = control.lastDataQ
			dataSlope = control.lastDataSlope
			prefixes = control.lastSignalPrefixes
			signalReason = "retained-no-live-prefix-signal"
		} else {
			control.mu.Unlock()
			core.Log.Debug(s, "DT rate-control skipped",
				"face", upstreamFace,
				"reason", "no-live-prefix-signal",
				"prefixes", prefixes)
			return
		}
	} else {
		control.lastInterestQ = interestQ
		control.lastInterestSlope = interestSlope
		control.lastDataQ = dataQ
		control.lastDataSlope = dataSlope
		control.lastSignalPrefixes = prefixes
		control.lastSignalReady = true
	}
	control.mu.Unlock()
	_ = signalReason

	// Default dominant signal is data. Switch to interest only when interest is strictly larger.
	currentQ := dataQ
	queueSlope := dataSlope
	selectedSignal := "data"
	if interestQ > dataQ {
		currentQ = interestQ
		queueSlope = interestSlope
		selectedSignal = "interest"
	}
	roundNow := time.Now()
	estimatedBWFace, measuredBWFace, bwSamplesFace, bwUpdateRule := s.getDtFaceBandwidthEstimate(upstreamFace, roundNow)

	pendingFace := s.pendingDtInterestsForFace(upstreamFace)
	control.mu.Lock()
	ensureDtControlThreadSlotsLocked(control)
	prevRate := control.rate
	measuredSendPps := 0.0
	hadPrevRateSample := !control.lastRateSampleTime.IsZero()
	if hadPrevRateSample {
		elapsed := roundNow.Sub(control.lastRateSampleTime).Seconds()
		if elapsed > 0 {
			delta := control.sentCounter - control.lastRateSampleCount
			measuredSendPps = float64(delta) / elapsed
		}
	}
	roundTokensNow := control.tokens
	roundMintedTokens := control.roundMintedTokens
	csvSink := s.getDtFaceCsvSinkLocked(control, upstreamFace)
	threadGranted := append([]uint64(nil), control.roundGrantedByThread...)
	threadUsed := append([]uint64(nil), control.roundUsedByThread...)
	threadZeroBudget := append([]uint64(nil), control.roundZeroBudgetByThread...)
	threadEmptyBudget := append([]uint64(nil), control.roundEmptyBudgetByThread...)
	threadSendFail := append([]uint64(nil), control.roundSendFailByThread...)
	threadPendingSum := append([]uint64(nil), control.roundPendingSumByThread...)
	threadPendingMax := append([]int(nil), control.roundPendingMaxByThread...)
	threadVisits := append([]uint64(nil), control.roundVisitsByThread...)
	for i := range control.roundGrantedByThread {
		control.roundGrantedByThread[i] = 0
		control.roundUsedByThread[i] = 0
		control.roundZeroBudgetByThread[i] = 0
		control.roundEmptyBudgetByThread[i] = 0
		control.roundSendFailByThread[i] = 0
		control.roundPendingSumByThread[i] = 0
		control.roundPendingMaxByThread[i] = 0
		control.roundVisitsByThread[i] = 0
	}
	control.roundMintedTokens = 0
	currentRate := prevRate
	if currentRate <= 0 {
		currentRate = dtRateControlParams.FwdMinRate
	}
	frozenRound := control.lastSignalReady && livePrefixes <= 0
	if frozenRound {
		controlState = "frozen"
		currentRate = prevRate
		bwUpdateRule = "no-qsf-sample-retain"
	}
	bwState := getDtFaceBandwidthState(upstreamFace)
	bwState.mu.Lock()
	bwCapEligible := bwState.capEligible
	bwState.mu.Unlock()
	if !control.bwCapActive && bwCapEligible && estimatedBWFace >= dtFaceInitRate {
		control.bwCapActive = true
		core.Log.Debug(s, "DT face BW cap activated",
			"face", upstreamFace,
			"estimatedBW_Pps", dtLogFloat1(estimatedBWFace),
			"estimatedBW_Mbps", dtLogFloat1(dtRatePpsToMbps(estimatedBWFace)),
			"initRatePps", dtLogFloat1(dtFaceInitRate),
			"initRateMbps", dtLogFloat1(dtRatePpsToMbps(dtFaceInitRate)))
	}
	bwCapActive := control.bwCapActive

	// Step 3: Define thresholds.
	qthHigh := dtRateControlParams.MaxFwdQueueSize * dtRateControlParams.FwdQueueBeta
	qthLow := dtRateControlParams.MaxFwdQueueSize * dtRateControlParams.FwdQueueAlpha
	slopeThreshold := dtRateControlParams.FwdSlopeThreshold

	// Step 4: State-machine controller.
	newRate := currentRate
	primaryAction := DtActionHoldDraining
	if frozenRound {
		newRate = currentRate
		primaryAction = control.action
	} else if currentQ >= qthHigh {
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
			newRate = currentRate * dtRateControlParams.FwdCIFactor
			primaryAction = DtActionCautiousIncrease
		} else {
			newRate = currentRate * dtRateControlParams.FwdRPFactor
			primaryAction = DtActionAggressiveProbe
		}
	}

	// Step 5: Bandwidth safety guard.
	finalAction := primaryAction
	if frozenRound {
		finalAction = control.action
	} else if bwCapActive && estimatedBWFace > 0 {
		maxSafeRate := estimatedBWFace * dtRateControlParams.MaxBWSafetyRatio
		if newRate > maxSafeRate {
			newRate = maxSafeRate
			finalAction = DtActionBWCapped
		}

		if currentQ < qthLow {
			minSafeRate := estimatedBWFace * dtRateControlParams.MinBWSafetyRatio
			if newRate < minSafeRate {
				newRate = minSafeRate
				finalAction = DtActionBWBoosted
			}
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

	// Liveness guard:
	// If this face has pending demand but measured send rate is zero in this control round,
	// prime a tiny forced packet budget for upcoming scheduler ticks.
	if hadPrevRateSample && pendingFace > 0 && measuredSendPps <= 0 {
		control.starvationZeroSendRounds++
		control.livenessForcedPackets += dtLivenessForcedPacketsPerStarvedRound
		if control.livenessForcedPackets > dtLivenessForcedPacketsMax {
			control.livenessForcedPackets = dtLivenessForcedPacketsMax
		}
	} else if pendingFace == 0 || measuredSendPps > 0 {
		control.starvationZeroSendRounds = 0
	}
	starvationRounds := control.starvationZeroSendRounds
	livenessForcedPackets := control.livenessForcedPackets

	// Step 7: persist and emit concise per-round telemetry.
	control.lastRateSampleTime = roundNow
	control.lastRateSampleCount = control.sentCounter
	control.lastRoundMeasuredSendPps = measuredSendPps
	if !frozenRound {
		control.rate = newRate
		control.action = finalAction
		control.lastRoundSignalQ = currentQ
		control.lastRoundSignalSlope = queueSlope
		control.lastRoundSignalType = selectedSignal
		control.lastRoundInterestQ = interestQ
		control.lastRoundInterestSlope = interestSlope
		control.lastRoundDataQ = dataQ
		control.lastRoundDataSlope = dataSlope
		control.lastRoundEstimatedBW = estimatedBWFace
		control.lastRoundMeasuredBW = measuredBWFace
		control.lastRoundBwSamples = bwSamplesFace
		control.lastRoundBwRule = bwUpdateRule
		control.lastRoundPrevRate = prevRate
		control.lastRoundNewRate = newRate
		control.lastRoundAction = finalAction
		control.lastRoundReady = true
	}
	control.mu.Unlock()

	bwMbpsFace := dtRatePpsToMbps(estimatedBWFace)
	sendMbpsMeasured := dtRatePpsToMbps(measuredSendPps)
	rateGapPps := prevRate - measuredSendPps
	sendLimit := dtSendLimitReason(prevRate, measuredSendPps, pendingFace)
	rateAdjustPps := newRate - prevRate
	_ = bwMbpsFace
	_ = sendMbpsMeasured
	_ = rateGapPps
	_ = rateAdjustPps
	csvAction := dtRateActionString(finalAction)
	if frozenRound {
		csvAction = "none"
	}
	roundGrantedTotal := uint64(0)
	roundUsedTotal := uint64(0)
	roundZeroBudgetTotal := uint64(0)
	roundEmptyBudgetTotal := uint64(0)
	roundSendFailTotal := uint64(0)
	roundVisitsTotal := uint64(0)
	threadPendingAvg := make([]float64, len(threadVisits))
	for i := range threadVisits {
		roundGrantedTotal += threadGranted[i]
		roundUsedTotal += threadUsed[i]
		roundZeroBudgetTotal += threadZeroBudget[i]
		roundEmptyBudgetTotal += threadEmptyBudget[i]
		roundSendFailTotal += threadSendFail[i]
		roundVisitsTotal += threadVisits[i]
		if threadVisits[i] > 0 {
			threadPendingAvg[i] = float64(threadPendingSum[i]) / float64(threadVisits[i])
		}
	}
	_ = roundTokensNow
	_ = roundMintedTokens
	_ = roundGrantedTotal
	_ = roundUsedTotal
	_ = roundZeroBudgetTotal
	_ = roundEmptyBudgetTotal
	_ = roundSendFailTotal
	_ = roundVisitsTotal
	_ = threadPendingAvg
	_ = threadPendingMax
	if starvationRounds >= dtLivenessStarvationLogThreshold {
		core.Log.Warn(s, "DT scheduler starvation detected",
			"face", upstreamFace,
			"pendingFace", pendingFace,
			"sendPpsMeasured", dtLogFloat1(measuredSendPps),
			"ratePrev", dtLogFloat1(prevRate),
			"rateNew", dtLogFloat1(newRate),
			"sendLimit", sendLimit,
			"starvationRounds", starvationRounds,
			"livenessForcedPackets", livenessForcedPackets)
	}

	/*
		core.Log.Debug(s, "DT rate-control round",
			"face", upstreamFace,
			"controlState", controlState,
			"prefixes", prefixes,
			"signalReason", signalReason,
			"signalType", selectedSignal,
			"signalQ", dtLogFloat1(currentQ),
			"signalSlope", dtLogFloat1(queueSlope),
			"signalInterestQ", dtLogFloat1(interestQ),
			"signalInterestSlope", dtLogFloat1(interestSlope),
			"signalDataQ", dtLogFloat1(dataQ),
			"signalDataSlope", dtLogFloat1(dataSlope),
			"bwPps", dtLogFloat1(estimatedBWFace),
			"bwMeasuredPps", dtLogFloat1(measuredBWFace),
			"bwSamples", bwSamplesFace,
			"bwUpdateRule", bwUpdateRule,
			"bwMbps", dtLogFloat1(bwMbpsFace),
			"sendPps", dtLogFloat1(measuredSendPps),
			"sendMbps", dtLogFloat1(sendMbpsMeasured),
			"sendLimit", sendLimit,
			"pendingFace", pendingFace,
			"starvationRounds", starvationRounds,
			"livenessForcedPackets", livenessForcedPackets,
			"ratePrev", dtLogFloat1(prevRate),
			"rateNew", dtLogFloat1(newRate),
			"rateGapPps", dtLogFloat1(rateGapPps),
			"rateAdjustPps", dtLogFloat1(rateAdjustPps),
			"action", dtRateActionString(finalAction))
	*/
	s.writeDtFaceCsvRound(
		csvSink,
		roundNow,
		controlState,
		csvAction,
		interestQ,
		interestSlope,
		dataQ,
		dataSlope,
		estimatedBWFace,
		bwSamplesFace,
		measuredSendPps,
		prevRate,
		newRate,
	)
	if sendLimit == "scheduler-limited" {
		/*
			core.Log.Debug(s, "DT scheduler round",
				"face", upstreamFace,
				"period", control.period,
				"ratePrev", dtLogFloat1(prevRate),
				"sendPps", dtLogFloat1(measuredSendPps),
				"pendingFace", pendingFace,
				"tokensNow", dtLogFloat1(roundTokensNow),
				"mintedTokens", dtLogFloat1(roundMintedTokens),
				"grantedPackets", roundGrantedTotal,
				"usedPackets", roundUsedTotal,
				"unusedGrantedPackets", roundGrantedTotal-roundUsedTotal,
				"zeroBudgetTicks", roundZeroBudgetTotal,
				"emptyBudgetPackets", roundEmptyBudgetTotal,
				"sendFailPackets", roundSendFailTotal,
				"threadVisits", threadVisits,
				"threadGranted", threadGranted,
				"threadUsed", threadUsed,
				"threadZeroBudgetTicks", threadZeroBudget,
				"threadEmptyBudgetPackets", threadEmptyBudget,
				"threadSendFailPackets", threadSendFail,
				"threadPendingAvg", threadPendingAvg,
				"threadPendingMax", threadPendingMax)
		*/
	}
}

// pendingDtInterestsForFace returns the number of queued DT interests (all threads, all prefixes)
// that are currently eligible to be sent toward upstreamFace.
func (s *Multipath) pendingDtInterestsForFace(upstreamFace uint64) int {
	control := getDtFaceControlState(upstreamFace)
	control.mu.Lock()
	defer control.mu.Unlock()
	ensureDtControlThreadSlotsLocked(control)
	total := 0
	for _, demand := range control.demandByThread {
		total += demand
	}
	return total
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

		info.DtValidFaces = flattenDtTierFaces(info.TierList)
		info.DtFaceRateControlEnabled = len(info.DtValidFaces) > 1
		info.DtBootstrapFaceRR = 0
		info.DtBootstrapProbeCounter = 0
		s.armDtFailureAfterPdConvergence(info, time.Now())
		if info.DtFaceRateControlEnabled {
			s.ensureDtControlActivated(info.Prefix, info.DtValidFaces, time.Now())
		} else {
			core.Log.Info(s, "DT prefix bypasses face rate control",
				"prefix", info.Prefix,
				"validFaces", info.DtValidFaces)
		}
		// ----------------------------------------

		core.Log.Info(s, "Local Node Converged", "prefix", info.Prefix)
	}
}

func flattenDtTierFaces(tiers [][]hopSnapshot) []uint64 {
	seen := make(map[uint64]bool)
	faces := make([]uint64, 0, 8)
	for _, tier := range tiers {
		for _, hop := range tier {
			if seen[hop.Nexthop] {
				continue
			}
			seen[hop.Nexthop] = true
			faces = append(faces, hop.Nexthop)
		}
	}
	return faces
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
