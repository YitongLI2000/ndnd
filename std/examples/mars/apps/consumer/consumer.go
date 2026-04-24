package main

import (
	"container/heap"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/named-data/ndnd/std/datapacket"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/engine"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/types/optional"
	"github.com/named-data/ndnd/std/utils"
)

// --- Configuration Constants ---

const (
	// Global Safety Limits
	TOTAL_DURATION    = 30 * time.Second
	INTEREST_LIFETIME = 4 * time.Second
	TICKER_INTERVAL   = 1 * time.Millisecond

	// RTT & Retransmission Defaults (DT Only)
	RTT_WINDOW_DURATION = 600 * time.Millisecond
	MIN_RTO             = 120.0
	MAX_RTO             = 800.0
	// DT control-update period = DT_CONTROL_PERIOD_RTT_SCALE * avg bootstrap RTT.
	DT_CONTROL_PERIOD_RTT_SCALE = 1.0
)

// --- Path Discovery (PD) Configuration ---
const (
	// Max pd rate is 20000 interests/sec
	PD_INIT_RATE        = 2000.0
	PD_INIT_RTO         = 200.0
	PD_REHEARSAL_BUDGET = 20
)

// --- Data Transmission (DT) Configuration ---
const (
	RC_RTT_LOW_THRESH_MULT  = 3.0
	RC_RTT_HIGH_THRESH_MULT = 6.0
	RC_INC_FACTOR           = 1.05
	RC_DEC_FACTOR           = 0.95
	RC_CAP_LOWER_MULT       = 0.8
	RC_CAP_UPPER_MULT       = 1.75
)

type ConsumerRateControlMode int

const (
	ConsumerRateControlDelay ConsumerRateControlMode = iota
	ConsumerRateControlQsf
)

type ConsumerQsfRateControlAction int

const (
	ConsumerQsfActionEmergencyDecrease ConsumerQsfRateControlAction = iota
	ConsumerQsfActionHoldDraining
	ConsumerQsfActionGentleDecrease
	ConsumerQsfActionCautiousIncrease
	ConsumerQsfActionAggressiveProbe
	ConsumerQsfActionBWCapped
	ConsumerQsfActionBWBoosted
	ConsumerQsfActionGlobalMinCapped
	ConsumerQsfActionGlobalMaxCapped
)

type ConsumerQsfRateControlParams struct {
	MaxQueueSize      float64
	QueueAlpha        float64
	QueueBeta         float64
	MDFactor          float64
	RPFactor          float64
	GDFactor          float64
	CIFactor          float64
	SlopeThreshold    float64
	MaxBWSafetyRatio  float64
	MinBWSafetyRatio  float64
	MinRate           float64
	MaxRate           float64
	SlopeTauRatio     float64
	NoSampleWarnEvery time.Duration
}

const consumerRateControlMode = ConsumerRateControlQsf

var consumerQsfRateControlParams = ConsumerQsfRateControlParams{
	MaxQueueSize:      1.0,
	QueueAlpha:        0.5,
	QueueBeta:         0.8,
	MDFactor:          0.9,
	RPFactor:          1.05,
	GDFactor:          0.95,
	CIFactor:          1.02,
	SlopeThreshold:    0.1,
	MaxBWSafetyRatio:  1.5,
	MinBWSafetyRatio:  0.5,
	MinRate:           0.005,
	MaxRate:           0.0,
	SlopeTauRatio:     1.0 / 3.0,
	NoSampleWarnEvery: 500 * time.Millisecond,
}

// Historical hardcoded DT tuning (for DataPacketValueCount=150):
// MAX_PACKETS = 15000
// DT_INIT_RATE = 3000.0
// THROUGHPUT_WINDOW_DUR = 20 * time.Millisecond
// RC_MIN_RATE = 10.0
const (
	baseMaxPackets         = 15000
	baseDTInitRate         = 3000.0
	baseThroughputWindow   = 30 * time.Millisecond
	baseRCMinRate          = 10.0
	baseDataPacketSizeByte = 16 + (150 * 8) // InterestQsf + DataQsf + 150 float64 values
)

var (
	MAX_PACKETS           = baseMaxPackets
	DT_INIT_RATE          = baseDTInitRate
	THROUGHPUT_WINDOW_DUR = baseThroughputWindow
	RC_MIN_RATE           = baseRCMinRate
)

// --- Asynchronous Workload Settings ---
const (
	WORKLOAD_TYPE        = "asynchronous" // "synchronous" or "asynchronous"
	SYNCHRONIZATION_TIME = 1001500        // microseconds
)

type ConsumerTag struct{}

func (ConsumerTag) String() string { return "NDN-Consumer" }

var consumerTag = ConsumerTag{}

func consumerLogFloat1(v float64) float64 {
	return math.Round(v*10) / 10
}

type TransmissionMode int

const (
	ModePD TransmissionMode = iota
	ModeDT
)

var (
	// Global mode setting for initialization
	initialMode      = ModePD
	consumerNodeName string
	consumerIndex    int

	// Global Synchronization for PD -> DT transition
	pdFinishedCount int
	pdTotalFlows    int
	pdMutex         sync.Mutex
	startDtCh       chan struct{}
)

func applyAdaptiveDtTunables() {
	currentPktSize := datapacket.NewDataPacket().GetSize()
	if currentPktSize <= 0 {
		log.Warn(consumerTag, "Adaptive DT tuning disabled: invalid packet size", "size", currentPktSize)
		return
	}

	scale := float64(baseDataPacketSizeByte) / float64(currentPktSize)
	if scale <= 0 {
		log.Warn(consumerTag, "Adaptive DT tuning disabled: invalid scale", "scale", scale)
		return
	}

	DT_INIT_RATE = math.Max(1.0, baseDTInitRate*scale)
	RC_MIN_RATE = math.Max(1.0, baseRCMinRate*scale)
	MAX_PACKETS = int(math.Max(1.0, math.Round(float64(baseMaxPackets)*scale)))
	// Keep DT sliding-window duration static (no chunk-size shaping).
	THROUGHPUT_WINDOW_DUR = baseThroughputWindow

	log.Info(consumerTag, "Adaptive DT tuning applied",
		"dataPacketSizeBytes", currentPktSize,
		"scale", scale,
		"DT_INIT_RATE", DT_INIT_RATE,
		"RC_MIN_RATE", RC_MIN_RATE,
		"MAX_PACKETS", MAX_PACKETS,
		"THROUGHPUT_WINDOW_DUR", THROUGHPUT_WINDOW_DUR)
}

// --- Packet Structures ---

type PacketSample struct {
	Timestamp   time.Time
	Size        int
	RTT         float64
	InterestQsf float64
}

type RTTSample struct {
	Timestamp time.Time
	RTT       float64
}

type PendingSend struct {
	SendTime      time.Time
	Retransmitted bool
}

type FlowContext struct {
	mu       sync.Mutex
	Prefix   string
	baseName enc.Name
	Mode     TransmissionMode

	// --- Shared State ---
	currentRate  float64
	rto          float64
	pendingSends map[string]PendingSend

	totalBytes        int64
	pktsReceivedTotal int
	startTime         time.Time
	endTime           time.Time
	isFinished        bool
	doneCh            chan struct{}

	// --- Path Discovery (PD) Specific State ---
	CurrentTier    int
	IsPaused       bool
	StopPD         bool
	OutstandingPD  int
	RehearsalCount int
	HasRehearsed   bool
	PdStartTime    time.Time
	PdFinished     bool

	// --- Data Transmission (DT) Specific State ---
	rttHistory            []RTTSample
	srtt, rttVar          float64
	window                []PacketSample
	estimatedBandwidth    float64
	lastMeasuredBandwidth float64
	lastBWUpdateRule      string
	lastRateSampleTime    time.Time
	lastNoSampleWarn      time.Time
	pktsCalibrated        int
	initialRTTSum         float64
	baseRTT               float64
	adjustmentPeriod      time.Duration
	lastRateUpdate        time.Time
	interestsSentInPeriod int
	retxQueue             []int
	retxQueued            map[int]bool

	// Reset signal for runFlow loop
	resetSignal chan struct{}
}

func NewFlowContext(prefix string) *FlowContext {
	now := time.Now()

	f := &FlowContext{
		Prefix:       prefix,
		startTime:    now,
		endTime:      now,
		doneCh:       make(chan struct{}),
		pendingSends: make(map[string]PendingSend),
		retxQueue:    make([]int, 0),
		retxQueued:   make(map[int]bool),
		resetSignal:  make(chan struct{}, 1),
	}

	if initialMode == ModePD {
		f.Mode = ModePD
		f.currentRate = PD_INIT_RATE
		f.rto = PD_INIT_RTO

		f.CurrentTier = 0
		f.IsPaused = false
		f.StopPD = false
		f.OutstandingPD = 0
		f.RehearsalCount = 0
		f.HasRehearsed = false
		f.PdStartTime = now
		f.PdFinished = false
		f.updatePDBaseName(0)
	} else {
		f.Mode = ModeDT
		f.initDTState()
	}

	return f
}

func (f *FlowContext) initDTState() {
	now := time.Now()

	f.currentRate = DT_INIT_RATE
	f.rto = MIN_RTO * 2

	f.startTime = now
	f.endTime = now
	f.totalBytes = 0
	f.pktsReceivedTotal = 0
	f.isFinished = false
	f.pendingSends = make(map[string]PendingSend)
	f.retxQueue = make([]int, 0)
	f.retxQueued = make(map[int]bool)
	f.rttHistory = make([]RTTSample, 0)
	f.estimatedBandwidth = DT_INIT_RATE
	f.lastMeasuredBandwidth = 0
	f.lastBWUpdateRule = "init"
	f.lastRateSampleTime = time.Time{}
	f.lastNoSampleWarn = time.Time{}
	f.window = make([]PacketSample, 0)
	f.pktsCalibrated = 0
	f.initialRTTSum = 0
	f.baseRTT = 0
	f.srtt = 0
	f.rttVar = 0
	f.interestsSentInPeriod = 0
	f.lastRateUpdate = now

	n, _ := enc.NameFromStr(f.Prefix)
	if consumerNodeName != "" {
		n = n.Append(enc.NewStringComponent(0x08, consumerNodeName))
	}
	n = n.Append(enc.NewStringComponent(0x08, "dt"))
	f.baseName = n
}

func (f *FlowContext) updatePDBaseName(tier int) {
	n, _ := enc.NameFromStr(f.Prefix)
	if consumerNodeName != "" {
		n = n.Append(enc.NewStringComponent(0x08, consumerNodeName))
	}
	n = n.Append(enc.NewStringComponent(0x08, "pd"))
	n = n.Append(enc.NewStringComponent(0x08, strconv.Itoa(tier)))
	f.baseName = n
}

func (f *FlowContext) GetRate() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.currentRate
}

func (f *FlowContext) GetRTO() float64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rto
}

func (f *FlowContext) RecordInterestSend(name string, t time.Time, isRetransmission bool) {
	f.mu.Lock()
	entry, exists := f.pendingSends[name]
	if !exists {
		// First transmission for this name.
		f.pendingSends[name] = PendingSend{
			SendTime:      t,
			Retransmitted: isRetransmission,
		}
	} else if isRetransmission {
		// Karn's algorithm: keep original send timestamp and mark retransmitted.
		entry.Retransmitted = true
		f.pendingSends[name] = entry
	} else {
		// Fresh transmission path (non-retransmission).
		f.pendingSends[name] = PendingSend{
			SendTime:      t,
			Retransmitted: false,
		}
	}

	if f.Mode == ModeDT {
		// Retransmissions are also token-controlled and consume link capacity, so
		// include them in the measured send rate printed by the control loop.
		f.interestsSentInPeriod++
	} else if !isRetransmission {
		f.OutstandingPD++
	}
	f.mu.Unlock()
}

func (f *FlowContext) QueueRetransmission(seq int) bool {
	if seq <= 0 {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Mode != ModeDT || f.isFinished || f.retxQueued[seq] {
		return false
	}
	f.retxQueued[seq] = true
	f.retxQueue = append(f.retxQueue, seq)
	return true
}

func (f *FlowContext) PopRetransmission() (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for len(f.retxQueue) > 0 {
		seq := f.retxQueue[0]
		copy(f.retxQueue[0:], f.retxQueue[1:])
		f.retxQueue[len(f.retxQueue)-1] = 0
		f.retxQueue = f.retxQueue[:len(f.retxQueue)-1]
		if !f.retxQueued[seq] {
			continue
		}
		delete(f.retxQueued, seq)
		if f.Mode != ModeDT || f.isFinished {
			continue
		}
		if _, pending := f.pendingSends[f.interestNameForSeq(seq)]; !pending {
			continue
		}
		return seq, true
	}
	return 0, false
}

func (f *FlowContext) interestNameForSeq(seq int) string {
	return f.baseName.Append(enc.NewStringComponent(0x08, "seq-"+strconv.Itoa(seq))).String()
}

func (f *FlowContext) ClearQueuedRetransmission(seq int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clearQueuedRetransmissionLocked(seq)
}

// THREAD SAFETY: caller must hold f.mu.
func (f *FlowContext) clearQueuedRetransmissionLocked(seq int) {
	if seq <= 0 {
		return
	}
	delete(f.retxQueued, seq)
}

// --- Core Event Handlers ---

// OnNack handles Nack packets.
func (f *FlowContext) OnNack(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	log.Debug(consumerTag, "OnNack", "name", name, "mode", f.Mode)

	delete(f.pendingSends, name)

	if f.Mode == ModePD {
		if f.OutstandingPD > 0 {
			f.OutstandingPD--
		}
		log.Debug(consumerTag, "PD: Nack processed", "prefix", f.Prefix, "outstanding", f.OutstandingPD)
	}
}

// OnTimeout handles Application-Layer RTO Expiration.
// This is called by the RetransmissionController, NOT the NDN framework callback.
func (f *FlowContext) OnTimeout(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// 1. Clean up pending state
	delete(f.pendingSends, name)

	// 2. PD Logic (C++ Parity)
	if f.Mode == ModePD {
		// In C++: if (currentTierCount > 0) currentTierCount--;
		if f.OutstandingPD > 0 {
			f.OutstandingPD--
		}

		log.Debug(consumerTag, "PD: RTO Timeout processed", "prefix", f.Prefix, "outstanding", f.OutstandingPD)

		// Check state transition (Stop PD or Next Tier)
		// This logic exists in C++ OnTimeout to handle cases where the last packet times out
		f.checkPdState()
	} else if f.Mode == ModeDT {
		// DT Logic is mostly handled by the Controller re-queueing the packet.
		// This function is just for state cleanup if necessary.
		log.Debug(consumerTag, "DT: RTO Timeout processed", "prefix", f.Prefix)
	}
}

// OnData handles Data packets and returns true only when the Data completed a
// currently pending Interest. Duplicate or stale Data should not inflate stats.
func (f *FlowContext) OnData(payload []byte, dataName string, receiveTime time.Time) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	pending, ok := f.pendingSends[dataName]
	if !ok {
		return false
	}
	delete(f.pendingSends, dataName)

	if f.isFinished {
		return false
	}

	// =========================================================
	//               MODE A: PATH DISCOVERY (PD)
	// =========================================================
	if f.Mode == ModePD {
		if f.OutstandingPD > 0 {
			f.OutstandingPD--
		}

		seqNum := extractSeqNum(dataName)
		if seqNum >= 0 && seqNum%50 == 0 {
			log.Debug(consumerTag, "PD Data Received", "name", dataName, "tier", f.CurrentTier, "outstanding", f.OutstandingPD)
		}

		discoveryPkt, err := datapacket.DeserializeDiscovery(payload)
		if err != nil {
			log.Warn(consumerTag, "PD: Failed to deserialize packet", "err", err)
			return false
		}

		if discoveryPkt.NodeConverged && !f.StopPD {
			log.Debug(consumerTag, "PD: Node Converged detected", "prefix", f.Prefix)
			f.StopPD = true
		} else if discoveryPkt.TierConverged && !f.IsPaused {
			log.Debug(consumerTag, "PD: Tier Converged detected - Pausing", "prefix", f.Prefix, "tier", f.CurrentTier)
			f.IsPaused = true
		}

		if f.IsPaused && !discoveryPkt.TierConverged {
			log.Debug(consumerTag, "PD: False Convergence detected (Paused) - Resuming", "prefix", f.Prefix)
			f.IsPaused = false
			if !f.HasRehearsed {
				f.HasRehearsed = true
				f.RehearsalCount = PD_REHEARSAL_BUDGET
				log.Debug(consumerTag, "PD: Triggering Rehearsal", "prefix", f.Prefix)
			}
		}

		f.checkPdState()
		return true
	}

	// =========================================================
	//             MODE B: DATA TRANSMISSION (DT)
	// =========================================================
	if f.Mode == ModeDT {
		dataPacket, err := datapacket.DeserializeData(payload)
		if err != nil {
			log.Warn(consumerTag, "DT: Failed to deserialize packet", "err", err)
			return false
		}

		rttVal := float64(receiveTime.Sub(pending.SendTime).Nanoseconds()) / 1e6

		f.endTime = receiveTime
		f.totalBytes += int64(len(payload))
		f.pktsReceivedTotal++

		if f.pktsReceivedTotal >= MAX_PACKETS {
			f.isFinished = true
			close(f.doneCh)
		}

		seqNum := extractSeqNum(dataName)
		f.clearQueuedRetransmissionLocked(seqNum)
		if seqNum >= 0 && seqNum%200 == 0 {
			log.Debug(consumerTag, "DT Data Received", "name", dataName, "rtt", rttVal, "totalRx", f.pktsReceivedTotal)
		}

		// Karn's algorithm: do not update RTT/RTO estimator with ambiguous RTT samples
		// from Interests that were retransmitted. Keep retransmitted Data out of
		// rate control too, otherwise old send timestamps inflate RTT and collapse BW.
		if !pending.Retransmitted {
			f.updateRTT(receiveTime, rttVal)
			f.updateRateControl(receiveTime, len(payload), rttVal, dataPacket.InterestQsf)
		}
		return true
	}

	return false
}

func (f *FlowContext) checkPdState() {
	if f.StopPD && f.OutstandingPD == 0 {
		if !f.PdFinished {
			duration := time.Since(f.PdStartTime)
			log.Info(consumerTag, "PD: Finished", "prefix", f.Prefix, "duration", duration)
			f.PdFinished = true
			go checkAllPdFinished()
		}
		return
	}

	if f.IsPaused && f.OutstandingPD == 0 {
		f.CurrentTier++
		log.Debug(consumerTag, "PD: Tier Finished - Probing Next", "prefix", f.Prefix, "newTier", f.CurrentTier)
		f.IsPaused = false
		f.updatePDBaseName(f.CurrentTier)
	}
}

func (f *FlowContext) updateRTT(receiveTime time.Time, rttVal float64) {
	f.rttHistory = append(f.rttHistory, RTTSample{Timestamp: receiveTime, RTT: rttVal})
	validIdx := 0
	for i, s := range f.rttHistory {
		if receiveTime.Sub(s.Timestamp) <= RTT_WINDOW_DURATION {
			validIdx = i
			break
		}
	}
	f.rttHistory = f.rttHistory[validIdx:]

	count := len(f.rttHistory)
	var meanRTT, variance, calculatedRTO, finalRTO float64
	if count < 2 {
		meanRTT = rttVal
		variance = 0.0
		calculatedRTO = math.Max(MIN_RTO, 2*rttVal)
		finalRTO = 2 * calculatedRTO
	} else {
		var sum float64
		for _, s := range f.rttHistory {
			sum += s.RTT
		}
		meanRTT = sum / float64(count)
		var varSum float64
		for _, s := range f.rttHistory {
			diff := s.RTT - meanRTT
			varSum += diff * diff
		}
		variance = varSum / float64(count)
		stdDev := math.Sqrt(variance)
		calculatedRTO = meanRTT + 4*stdDev
		finalRTO = 2 * calculatedRTO
	}
	if finalRTO < MIN_RTO {
		finalRTO = MIN_RTO
	}
	if finalRTO > MAX_RTO {
		finalRTO = MAX_RTO
	}

	f.srtt = meanRTT
	f.rttVar = variance
	f.rto = finalRTO
}

func (f *FlowContext) updateRateControl(receiveTime time.Time, payloadSize int, rttVal float64, interestQsf float64) {
	f.lastRateSampleTime = receiveTime
	f.window = append(f.window, PacketSample{
		Timestamp:   receiveTime,
		Size:        payloadSize,
		RTT:         rttVal,
		InterestQsf: interestQsf,
	})
	f.trimRateControlWindow(receiveTime)

	if f.pktsCalibrated < 5 {
		f.initialRTTSum += rttVal
		f.pktsCalibrated++
		if f.pktsCalibrated == 5 {
			f.baseRTT = f.initialRTTSum / 5.0
			// Use scaled bootstrap RTT as the control-update period, but do not
			// allow very small RTTs to make the application control loop too noisy.
			f.adjustmentPeriod = time.Duration(f.baseRTT * DT_CONTROL_PERIOD_RTT_SCALE * float64(time.Millisecond))
			if f.adjustmentPeriod < 30*time.Millisecond {
				f.adjustmentPeriod = 30 * time.Millisecond
			}
			f.lastRateUpdate = receiveTime
			f.interestsSentInPeriod = 0
			log.Info(consumerTag, "Flow Calibration Complete",
				"flow", f.Prefix,
				"mode", consumerRateControlModeString(),
				"baseRTT", consumerLogFloat1(f.baseRTT),
				"controlPeriodScale", DT_CONTROL_PERIOD_RTT_SCALE,
				"adjustmentPeriod", f.adjustmentPeriod)
		}
		return
	}

	timeSinceLastUpdate := receiveTime.Sub(f.lastRateUpdate)
	if timeSinceLastUpdate < f.adjustmentPeriod {
		return
	}

	actualRate := 0.0
	if timeSinceLastUpdate.Seconds() > 0 {
		actualRate = float64(f.interestsSentInPeriod) / timeSinceLastUpdate.Seconds()
	}

	switch consumerRateControlMode {
	case ConsumerRateControlQsf:
		f.updateQsfRateControl(receiveTime, actualRate)
	default:
		f.updateDelayRateControl(receiveTime, actualRate)
	}

	f.interestsSentInPeriod = 0
}

func (f *FlowContext) trimRateControlWindow(now time.Time) {
	cutoff := now.Add(-THROUGHPUT_WINDOW_DUR)
	validWinIdx := len(f.window)
	for i, s := range f.window {
		if !s.Timestamp.Before(cutoff) {
			validWinIdx = i
			break
		}
	}
	if validWinIdx > 0 {
		f.window = f.window[validWinIdx:]
	}
}

func consumerRateControlModeString() string {
	switch consumerRateControlMode {
	case ConsumerRateControlQsf:
		return "qsf"
	default:
		return "delay"
	}
}

func consumerQsfActionString(action ConsumerQsfRateControlAction) string {
	switch action {
	case ConsumerQsfActionEmergencyDecrease:
		return "emergency-decrease"
	case ConsumerQsfActionHoldDraining:
		return "hold-draining"
	case ConsumerQsfActionGentleDecrease:
		return "gentle-decrease"
	case ConsumerQsfActionCautiousIncrease:
		return "cautious-increase"
	case ConsumerQsfActionAggressiveProbe:
		return "aggressive-probe"
	case ConsumerQsfActionBWCapped:
		return "bw-capped"
	case ConsumerQsfActionBWBoosted:
		return "bw-boosted"
	case ConsumerQsfActionGlobalMinCapped:
		return "global-min-capped"
	case ConsumerQsfActionGlobalMaxCapped:
		return "global-max-capped"
	default:
		return "unknown"
	}
}

func (f *FlowContext) checkRateControlNoFreshSamples(now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.Mode != ModeDT || f.pktsCalibrated < 5 || f.isFinished {
		return
	}

	f.trimRateControlWindow(now)
	if len(f.window) > 0 || f.estimatedBandwidth <= 0 {
		return
	}

	warnEvery := consumerQsfRateControlParams.NoSampleWarnEvery
	if warnEvery <= 0 {
		warnEvery = 500 * time.Millisecond
	}
	if !f.lastNoSampleWarn.IsZero() && now.Sub(f.lastNoSampleWarn) < warnEvery {
		return
	}

	staleFor := time.Duration(0)
	if !f.lastRateSampleTime.IsZero() {
		staleFor = now.Sub(f.lastRateSampleTime)
	}
	lastRule := f.lastBWUpdateRule
	f.lastNoSampleWarn = now
	log.Warn(consumerTag, "DT BW estimate retained without fresh samples",
		"prefix", f.Prefix,
		"retainedBWPps", consumerLogFloat1(f.estimatedBandwidth),
		"lastMeasuredBWPps", consumerLogFloat1(f.lastMeasuredBandwidth),
		"lastUpdateRule", lastRule,
		"staleFor", staleFor)
}

func (f *FlowContext) windowPayloadAvgBytes() float64 {
	if len(f.window) == 0 {
		return 0
	}
	totalBytes := 0
	for _, s := range f.window {
		totalBytes += s.Size
	}
	return float64(totalBytes) / float64(len(f.window))
}

func (f *FlowContext) measuredPPSFromWindow() float64 {
	if len(f.window) == 0 || THROUGHPUT_WINDOW_DUR <= 0 {
		return 0
	}
	return float64(len(f.window)) / THROUGHPUT_WINDOW_DUR.Seconds()
}

func (f *FlowContext) updateDelayRateControl(receiveTime time.Time, actualRate float64) {
	var totalWinRTT float64
	for _, s := range f.window {
		totalWinRTT += s.RTT
	}
	avgWinRTT := totalWinRTT / float64(len(f.window))
	measuredPPS := f.measuredPPSFromWindow()

	bwUpdateRule := "retain"
	if measuredPPS > f.estimatedBandwidth {
		f.estimatedBandwidth = measuredPPS
		bwUpdateRule = "throughput-high"
	}
	if avgWinRTT > RC_RTT_LOW_THRESH_MULT*f.baseRTT {
		f.estimatedBandwidth = measuredPPS
		bwUpdateRule = "rtt-pressure"
	}
	f.lastMeasuredBandwidth = measuredPPS
	f.lastBWUpdateRule = bwUpdateRule

	targetRate := f.currentRate
	if avgWinRTT < RC_RTT_LOW_THRESH_MULT*f.baseRTT {
		targetRate = f.currentRate * RC_INC_FACTOR
	} else if avgWinRTT >= RC_RTT_HIGH_THRESH_MULT*f.baseRTT {
		targetRate = f.estimatedBandwidth * RC_DEC_FACTOR
	}

	lower := RC_CAP_LOWER_MULT * f.estimatedBandwidth
	if lower < 1.0 {
		lower = 1.0
	}
	upper := RC_CAP_UPPER_MULT * f.estimatedBandwidth
	if upper < DT_INIT_RATE {
		upper = DT_INIT_RATE
	}

	if targetRate < lower {
		targetRate = lower
	}
	if targetRate > upper {
		targetRate = upper
	}
	if targetRate < RC_MIN_RATE {
		targetRate = RC_MIN_RATE
	}

	oldRate := f.currentRate
	f.currentRate = targetRate
	f.lastRateUpdate = receiveTime
	avgPayloadBytes := f.windowPayloadAvgBytes()
	estimatedBWMbps := (f.estimatedBandwidth * avgPayloadBytes * 8.0) / 1e6

	log.Debug(consumerTag, "Rate Adjusted",
		"mode", "delay",
		"prefix", f.Prefix,
		"oldTargetRate", consumerLogFloat1(oldRate),
		"newTargetRate", consumerLogFloat1(f.currentRate),
		"actualRate", consumerLogFloat1(actualRate),
		"measuredPPS", consumerLogFloat1(measuredPPS),
		"estimatedBW", consumerLogFloat1(f.estimatedBandwidth),
		"estimatedBWMbps", consumerLogFloat1(estimatedBWMbps),
		"bwUpdateRule", bwUpdateRule,
		"baseRTT", consumerLogFloat1(f.baseRTT),
		"latestRTT", consumerLogFloat1(avgWinRTT))
}

func (f *FlowContext) updateQsfRateControl(receiveTime time.Time, actualRate float64) {
	params := consumerQsfRateControlParams
	currentQ, queueSlope := f.computeInterestQsfSignal(receiveTime)
	measuredPPS := f.measuredPPSFromWindow()
	bwUpdateRule := f.updateQsfBandwidthEstimate(measuredPPS, currentQ)

	minRate := math.Max(params.MinRate, RC_MIN_RATE)
	currentRate := f.currentRate
	if currentRate <= 0 {
		currentRate = minRate
	}

	qthHigh := params.MaxQueueSize * params.QueueBeta
	qthLow := params.MaxQueueSize * params.QueueAlpha
	targetRate := currentRate
	action := ConsumerQsfActionHoldDraining

	if currentQ >= qthHigh {
		if queueSlope > params.SlopeThreshold {
			targetRate = currentRate * params.MDFactor
			action = ConsumerQsfActionEmergencyDecrease
		} else if queueSlope < -params.SlopeThreshold {
			targetRate = currentRate
			action = ConsumerQsfActionHoldDraining
		} else {
			targetRate = currentRate * params.GDFactor
			action = ConsumerQsfActionGentleDecrease
		}
	} else if currentQ >= qthLow {
		targetRate = currentRate
		action = ConsumerQsfActionHoldDraining
	} else {
		if queueSlope > params.SlopeThreshold {
			if f.estimatedBandwidth > 0 {
				targetRate = f.estimatedBandwidth * params.CIFactor
			} else {
				targetRate = currentRate * params.CIFactor
			}
			action = ConsumerQsfActionCautiousIncrease
		} else {
			if f.estimatedBandwidth > 0 {
				targetRate = f.estimatedBandwidth * params.RPFactor
			} else {
				targetRate = currentRate * params.RPFactor
			}
			action = ConsumerQsfActionAggressiveProbe
		}
	}

	if f.estimatedBandwidth > 0 {
		upper := f.estimatedBandwidth * params.MaxBWSafetyRatio
		if upper > 0 && targetRate > upper {
			targetRate = upper
			action = ConsumerQsfActionBWCapped
		}

		lower := f.estimatedBandwidth * params.MinBWSafetyRatio
		if lower > 0 && targetRate < lower {
			targetRate = lower
			action = ConsumerQsfActionBWBoosted
		}
	}

	if targetRate < minRate {
		targetRate = minRate
		action = ConsumerQsfActionGlobalMinCapped
	}
	if params.MaxRate > 0 && targetRate > params.MaxRate {
		targetRate = params.MaxRate
		action = ConsumerQsfActionGlobalMaxCapped
	}

	oldRate := f.currentRate
	f.currentRate = targetRate
	f.lastRateUpdate = receiveTime
	avgPayloadBytes := f.windowPayloadAvgBytes()
	estimatedBWMbps := (f.estimatedBandwidth * avgPayloadBytes * 8.0) / 1e6

	log.Debug(consumerTag, "Rate Adjusted",
		"mode", "qsf",
		"prefix", f.Prefix,
		"qsfAvg", consumerLogFloat1(currentQ),
		"qsfSlope", consumerLogFloat1(queueSlope),
		"oldTargetRate", consumerLogFloat1(oldRate),
		"newTargetRate", consumerLogFloat1(f.currentRate),
		"actualRate", consumerLogFloat1(actualRate),
		"measuredPPS", consumerLogFloat1(measuredPPS),
		"estimatedBW", consumerLogFloat1(f.estimatedBandwidth),
		"estimatedBWMbps", consumerLogFloat1(estimatedBWMbps),
		"bwUpdateRule", bwUpdateRule,
		"action", consumerQsfActionString(action))
}

func (f *FlowContext) computeInterestQsfSignal(now time.Time) (float64, float64) {
	if len(f.window) == 0 {
		return 0, 0
	}

	tau := THROUGHPUT_WINDOW_DUR.Seconds() * consumerQsfRateControlParams.SlopeTauRatio
	if tau <= 0 {
		tau = THROUGHPUT_WINDOW_DUR.Seconds() / 3.0
	}

	var avgQsf float64
	var sumW, sumWT, sumWQ, sumWTT, sumWTQ float64
	for _, sample := range f.window {
		qsf := sample.InterestQsf
		avgQsf += qsf

		t := sample.Timestamp.Sub(now).Seconds()
		w := math.Exp(t / tau)
		sumW += w
		sumWT += w * t
		sumWQ += w * qsf
		sumWTT += w * t * t
		sumWTQ += w * t * qsf
	}
	avgQsf /= float64(len(f.window))

	denom := sumW*sumWTT - sumWT*sumWT
	if math.Abs(denom) < 1e-9 {
		return avgQsf, 0
	}
	slope := (sumW*sumWTQ - sumWT*sumWQ) / denom
	return avgQsf, slope
}

func (f *FlowContext) updateQsfBandwidthEstimate(measuredPPS float64, queuePressureQsf float64) string {
	prevBW := f.estimatedBandwidth
	queueThreshold := consumerQsfRateControlParams.MaxQueueSize * consumerQsfRateControlParams.QueueAlpha
	rule := "retain"

	switch {
	case measuredPPS <= 0 && prevBW > 0:
		rule = "no-sample-retain"
	case prevBW <= 0:
		f.estimatedBandwidth = measuredPPS
		rule = "bootstrap"
	case measuredPPS > prevBW:
		f.estimatedBandwidth = measuredPPS
		rule = "new-throughput-high"
	case queuePressureQsf > queueThreshold:
		f.estimatedBandwidth = measuredPPS
		rule = "queue-pressure"
	default:
		f.estimatedBandwidth = prevBW
	}

	if measuredPPS <= 0 {
		f.lastMeasuredBandwidth = measuredPPS
		f.lastBWUpdateRule = rule
		return rule
	}

	f.lastMeasuredBandwidth = measuredPPS
	f.lastBWUpdateRule = rule
	return rule
}

func (f *FlowContext) GetFinalStats() (float64, int64, float64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	duration := f.endTime.Sub(f.startTime).Seconds()
	if duration <= 0 {
		return 0.0, f.totalBytes, 0.0
	}
	throughputMbps := (float64(f.totalBytes) * 8.0) / (duration * 1e6)
	return throughputMbps, f.totalBytes, duration
}

// --- Global Coordination ---

func checkAllPdFinished() {
	pdMutex.Lock()
	pdFinishedCount++
	allDone := pdFinishedCount == pdTotalFlows
	pdMutex.Unlock()

	if allDone {
		log.Info(consumerTag, "All flows finished PD. Preparing for DT...")
		close(startDtCh)
	}
}

func startDataTransmission(f *FlowContext) {
	delay := time.Duration(0)

	if WORKLOAD_TYPE == "asynchronous" {
		if consumerIndex%2 == 0 {
			delay = 0
			log.Info(consumerTag, "DT Start", "flow", f.Prefix, "type", "Even/Immediate")
		} else {
			delay = time.Duration(SYNCHRONIZATION_TIME) * time.Microsecond
			log.Info(consumerTag, "DT Start", "flow", f.Prefix, "type", "Odd/Delayed", "delay", delay)
		}
	}

	if delay > 0 {
		time.Sleep(delay)
	}

	f.mu.Lock()
	f.Mode = ModeDT
	f.initDTState()
	f.mu.Unlock()

	select {
	case f.resetSignal <- struct{}{}:
	default:
	}
}

// --- Statistics ---

type Statistics struct {
	mu              sync.Mutex
	totalSent       int
	dataReceived    int
	nackReceived    int
	timeoutReceived int
	retransmissions int
}

func (s *Statistics) IncrementSent()    { s.mu.Lock(); s.totalSent++; s.mu.Unlock() }
func (s *Statistics) IncrementData()    { s.mu.Lock(); s.dataReceived++; s.mu.Unlock() }
func (s *Statistics) IncrementNack()    { s.mu.Lock(); s.nackReceived++; s.mu.Unlock() }
func (s *Statistics) IncrementTimeout() { s.mu.Lock(); s.timeoutReceived++; s.mu.Unlock() }
func (s *Statistics) IncrementRetx()    { s.mu.Lock(); s.retransmissions++; s.mu.Unlock() }

func (s *Statistics) Print() {
	s.mu.Lock()
	defer s.mu.Unlock()
	log.Info(consumerTag, "=== Final Statistics ===",
		"totalSent", s.totalSent,
		"dataReceived", s.dataReceived,
		"nackReceived", s.nackReceived,
		"timeouts", s.timeoutReceived,
		"retransmissions", s.retransmissions)
}

// --- Heap-based Retransmission Controller ---

type PendingPacket struct {
	Name        string
	Prefix      string
	Expiration  time.Time
	SequenceNum int
	FlowCtx     *FlowContext
	Index       int
}

type PacketHeap []*PendingPacket

func (h PacketHeap) Len() int           { return len(h) }
func (h PacketHeap) Less(i, j int) bool { return h[i].Expiration.Before(h[j].Expiration) }
func (h PacketHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].Index = i; h[j].Index = j }
func (h *PacketHeap) Push(x interface{}) {
	n := len(*h)
	item := x.(*PendingPacket)
	item.Index = n
	*h = append(*h, item)
}
func (h *PacketHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.Index = -1
	*h = old[0 : n-1]
	return item
}

type RetransmissionController struct {
	mu       sync.Mutex
	pq       PacketHeap
	active   map[string]int
	app      ndn.Engine
	stats    *Statistics
	stopCh   chan struct{}
	wakeupCh chan struct{}
}

func NewRetransmissionController(app ndn.Engine, stats *Statistics) *RetransmissionController {
	rc := &RetransmissionController{
		pq:       make(PacketHeap, 0),
		active:   make(map[string]int),
		app:      app,
		stats:    stats,
		stopCh:   make(chan struct{}),
		wakeupCh: make(chan struct{}, 1),
	}
	heap.Init(&rc.pq)
	return rc
}

func (rc *RetransmissionController) Add(name string, prefix string, seq int, flowCtx *FlowContext) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rto := flowCtx.GetRTO()
	expiration := time.Now().Add(time.Duration(rto) * time.Millisecond)
	pkt := &PendingPacket{Name: name, Prefix: prefix, Expiration: expiration, SequenceNum: seq, FlowCtx: flowCtx}
	rc.active[name] = seq
	heap.Push(&rc.pq, pkt)
	select {
	case rc.wakeupCh <- struct{}{}:
	default:
	}
}

func (rc *RetransmissionController) Remove(name string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	delete(rc.active, name)
}

// TODO: The real retransmission implementation
func (rc *RetransmissionController) StartScheduler() {
	for {
		var waitDuration time.Duration
		rc.mu.Lock()
		if rc.pq.Len() == 0 {
			waitDuration = 10 * time.Second
		} else {
			now := time.Now()
			top := rc.pq[0]
			if now.After(top.Expiration) {
				// Packet has expired
				item := heap.Pop(&rc.pq).(*PendingPacket)

				// Check if still active (not Acked/Nacked yet)
				if seq, exists := rc.active[item.Name]; exists && seq == item.SequenceNum {

					// --- DT MODE: Retransmit ---
					if item.FlowCtx.Mode == ModeDT {
						queued := item.FlowCtx.QueueRetransmission(item.SequenceNum)
						log.Debug(consumerTag, "DT: RTO Expired - Queueing retransmission",
							"name", item.Name,
							"queued", queued)
						rc.mu.Unlock()
						continue
					} else {
						// --- PD MODE: Handle Timeout Logic (No Retransmit) ---
						log.Debug(consumerTag, "PD: RTO Expired - Handling Logic", "name", item.Name)

						// Remove from active tracking, we treat it as lost/timed out
						delete(rc.active, item.Name)
						rc.mu.Unlock()

						rc.stats.IncrementTimeout()
						// Call the FlowContext's OnTimeout to handle counters/state
						item.FlowCtx.OnTimeout(item.Name)
						continue
					}
				}
				// Item was already removed or stale, just drop
				rc.mu.Unlock()
				continue
			} else {
				waitDuration = top.Expiration.Sub(now)
			}
		}
		rc.mu.Unlock()
		select {
		case <-rc.stopCh:
			return
		case <-rc.wakeupCh:
			continue
		case <-time.After(waitDuration):
			continue
		}
	}
}

func (rc *RetransmissionController) Stop() { close(rc.stopCh) }

var globalRetxController *RetransmissionController

// --- Sending Logic ---

func sendSingleInterest(app ndn.Engine, stats *Statistics, sequenceNum int, flowCtx *FlowContext, isRetransmission bool) {
	name := flowCtx.baseName.Append(enc.NewStringComponent(0x08, "seq-"+strconv.Itoa(sequenceNum)))

	intCfg := &ndn.InterestConfig{
		MustBeFresh: true,
		Lifetime:    optional.Some(INTEREST_LIFETIME),
		Nonce:       utils.ConvertNonce(app.Timer().Nonce()),
	}

	interest, err := app.Spec().MakeInterest(name, intCfg, nil, nil)
	if err != nil {
		return
	}

	if !isRetransmission {
		stats.IncrementSent()
	} else {
		stats.IncrementRetx()
	}
	interestName := interest.FinalName.String()
	sendTime := time.Now()

	if sequenceNum%200 == 0 || isRetransmission {
		currentRate := flowCtx.GetRate()
		log.Debug(consumerTag, "Interest Sent",
			"name", interestName,
			"rate", currentRate,
			"isRetx", isRetransmission)
	}

	// Update pending sends map (needed for RTT calc in DT)
	flowCtx.RecordInterestSend(interestName, sendTime, isRetransmission)

	// Arm the next application RTO from the actual send time. This matters for
	// rate-controlled retransmissions because they may wait in retxQueue.
	globalRetxController.Add(interestName, flowCtx.Prefix, sequenceNum, flowCtx)

	err = app.Express(interest,
		func(args ndn.ExpressCallbackArgs) {
			receiveTime := time.Now()
			switch args.Result {
			case ndn.InterestResultNack:
				stats.IncrementNack()
				globalRetxController.Remove(interestName)
				flowCtx.ClearQueuedRetransmission(sequenceNum)
				flowCtx.OnNack(interestName)

			case ndn.InterestResultTimeout:
				// This is the NDN Framework Timeout (Lifetime ~4s).
				// The Application RTO (RetransmissionController) handles the "real" logic.
				// We just log this for debugging "dead" packets.
				log.Debug(consumerTag, "NDN Framework Timeout (Lifetime reached)", "name", interestName)
				// We should ensure the controller stops tracking this if it hasn't already.
				globalRetxController.Remove(interestName)
				flowCtx.ClearQueuedRetransmission(sequenceNum)
				// We do NOT call flowCtx.OnTimeout here, because the Controller likely already did
				// (or will do) the RTO logic.

			case ndn.InterestCancelled:
				globalRetxController.Remove(interestName)
				flowCtx.ClearQueuedRetransmission(sequenceNum)

			case ndn.InterestResultData:
				data := args.Data
				content := data.Content().Join()
				dataName := data.Name().String()
				// Always clear the exact outstanding key used by the retransmission controller.
				// This avoids stale entries if returned Data name differs from expressed Interest name.
				globalRetxController.Remove(interestName)
				if dataName != interestName {
					log.Debug(consumerTag, "Data name differs from Interest name",
						"interestName", interestName, "dataName", dataName)
					globalRetxController.Remove(dataName)
				}
				if flowCtx.OnData(content, interestName, receiveTime) {
					stats.IncrementData()
				}
			}
		})
}

// --- Main Flow Loop ---

func runFlow(app ndn.Engine, stats *Statistics, flowCtx *FlowContext, wg *sync.WaitGroup) {
	defer wg.Done()
	totalTimer := time.NewTimer(TOTAL_DURATION)
	defer totalTimer.Stop()
	ticker := time.NewTicker(TICKER_INTERVAL)
	defer ticker.Stop()

	tokens := 0.0
	sequenceNum := 0

	log.Info(consumerTag, "Starting flow", "target", flowCtx.Prefix, "mode", flowCtx.Mode)

	for {
		select {
		case <-totalTimer.C:
			return
		case <-flowCtx.doneCh:
			return
		case <-flowCtx.resetSignal:
			log.Info(consumerTag, "Flow resetting for DT", "prefix", flowCtx.Prefix)
			tokens = 0.0
			sequenceNum = 0

		case <-ticker.C:
			flowCtx.checkRateControlNoFreshSamples(time.Now())

			// PD Logic: Pause Check
			if flowCtx.Mode == ModePD {
				flowCtx.mu.Lock()
				if flowCtx.StopPD {
					flowCtx.mu.Unlock()
					continue
				}
				shouldSend := false
				if flowCtx.RehearsalCount > 0 {
					flowCtx.RehearsalCount--
					shouldSend = true
				} else {
					if !flowCtx.IsPaused {
						shouldSend = true
					}
				}
				flowCtx.mu.Unlock()

				if !shouldSend {
					continue
				}
			}

			currentRate := flowCtx.GetRate()
			tokens += currentRate * TICKER_INTERVAL.Seconds()

			for tokens >= 1.0 {
				if flowCtx.Mode == ModeDT {
					if retxSeq, ok := flowCtx.PopRetransmission(); ok {
						tokens -= 1.0
						sendSingleInterest(app, stats, retxSeq, flowCtx, true)
						continue
					}
				}

				if sequenceNum >= MAX_PACKETS {
					tokens = 0
					if flowCtx.Mode == ModePD {
						log.Warn(consumerTag, "PD: Max Packets Reached", "prefix", flowCtx.Prefix)
					}
					break
				}

				sequenceNum++
				tokens -= 1.0
				sendSingleInterest(app, stats, sequenceNum, flowCtx, false)
			}
		}
	}
}

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <node_name> <producer_range> <initialMode>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  initialMode must be either \"ModePD\" or \"ModeDT\"\n")
		os.Exit(1)
	}

	nodeName := os.Args[1]
	rangeStr := os.Args[2]
	modeStr := os.Args[3]
	consumerNodeName = nodeName

	// Parse initial mode from command line argument
	switch modeStr {
	case "ModePD":
		initialMode = ModePD
	case "ModeDT":
		initialMode = ModeDT
	default:
		fmt.Fprintf(os.Stderr, "Error: invalid initialMode \"%s\". Must be \"ModePD\" or \"ModeDT\"\n", modeStr)
		os.Exit(1)
	}

	if strings.HasPrefix(nodeName, "con") {
		idxStr := strings.TrimPrefix(nodeName, "con")
		if idx, err := strconv.Atoi(idxStr); err == nil {
			consumerIndex = idx
		}
	}

	applyAdaptiveDtTunables()

	targetPrefixes, err := parseProducerRange(rangeStr)
	if err != nil {
		log.Fatal(consumerTag, "Failed to parse producer range", "err", err)
		return
	}

	//! Change the log level of consumer
	log.Default().SetLevel(log.LevelDebug)
	// log.Default().SetLevel(log.LevelInfo)

	os.Setenv("NDN_CLIENT_TRANSPORT", "tcp://127.0.0.1:6363")

	app := engine.NewBasicEngine(engine.NewDefaultFace())
	err = app.Start()
	if err != nil {
		log.Fatal(consumerTag, "Unable to start engine", "err", err)
		return
	}
	defer app.Stop()

	stats := &Statistics{}
	globalRetxController = NewRetransmissionController(app, stats)
	go globalRetxController.StartScheduler()
	defer globalRetxController.Stop()

	log.Info(consumerTag, "Starting Optimized NDN Consumer",
		"nodeName", nodeName,
		"index", consumerIndex,
		"targets", len(targetPrefixes),
		"initialMode", initialMode)

	pdTotalFlows = len(targetPrefixes)
	startDtCh = make(chan struct{})

	var wg sync.WaitGroup
	startTime := time.Now()
	flowContexts := make([]*FlowContext, 0, len(targetPrefixes))

	for _, prefix := range targetPrefixes {
		wg.Add(1)
		flowCtx := NewFlowContext(prefix)
		flowContexts = append(flowContexts, flowCtx)
		go runFlow(app, stats, flowCtx, &wg)
	}

	go func() {
		<-startDtCh
		for _, f := range flowContexts {
			go startDataTransmission(f)
		}
	}()

	wg.Wait()
	elapsed := time.Since(startTime)
	log.Info(consumerTag, "All flows stopped", "elapsedTime", elapsed)

	time.Sleep(1 * time.Second)
	stats.Print()

	if initialMode == ModeDT || flowContexts[0].Mode == ModeDT {
		log.Info(consumerTag, "=== Final Per-Flow Statistics (DT) ===")
		for _, flowCtx := range flowContexts {
			avgThroughput, totalBytes, duration := flowCtx.GetFinalStats()
			log.Info(consumerTag, "Flow Summary",
				"flow", flowCtx.Prefix,
				"totalBytes", totalBytes,
				"avgMbps", avgThroughput,
				"duration", duration)
		}
	}
}

// Helpers

func extractSeqNum(nameStr string) int {
	parts := strings.Split(nameStr, "/")
	if len(parts) == 0 {
		return -1
	}
	lastPart := parts[len(parts)-1]
	if !strings.HasPrefix(lastPart, "seq-") {
		return -1
	}
	seqStr := strings.TrimPrefix(lastPart, "seq-")
	seqNum, err := strconv.Atoi(seqStr)
	if err != nil {
		return -1
	}
	return seqNum
}

func parseProducerRange(rangeStr string) ([]string, error) {
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid format")
	}
	start, err1 := strconv.Atoi(parts[0])
	end, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return nil, fmt.Errorf("invalid numbers")
	}
	var prefixes []string
	for i := start; i <= end; i++ {
		prefixes = append(prefixes, fmt.Sprintf("/pro%dapp", i))
	}
	return prefixes, nil
}
