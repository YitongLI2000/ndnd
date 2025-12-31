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

	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/engine"
	"github.com/named-data/ndnd/std/log"
	"github.com/named-data/ndnd/std/ndn"
	"github.com/named-data/ndnd/std/types/optional"
	"github.com/named-data/ndnd/std/utils"

	"ndnd-mars-three/internal/datapacket"
)

// --- Configuration Constants ---

const (
	// Global Safety Limits
	TOTAL_DURATION    = 30 * time.Second
	MAX_PACKETS       = 15000
	INTEREST_LIFETIME = 4 * time.Second
	TICKER_INTERVAL   = 1 * time.Millisecond

	// RTT & Retransmission Defaults (DT Only)
	RTT_WINDOW_DURATION = 1 * time.Second
	MIN_RTO             = 60.0
	MAX_RTO             = 600.0
)

// --- Path Discovery (PD) Configuration ---
const (
	PD_INIT_RATE        = 500.0
	PD_INIT_RTO         = 200.0
	PD_REHEARSAL_BUDGET = 20
)

// --- Data Transmission (DT) Configuration ---
const (
	DT_INIT_RATE          = 3000.0
	THROUGHPUT_WINDOW_DUR = 20 * time.Millisecond

	RC_RTT_LOW_THRESH_MULT  = 3.0
	RC_RTT_HIGH_THRESH_MULT = 6.0
	RC_INC_FACTOR           = 1.05
	RC_DEC_FACTOR           = 0.95
	RC_CAP_LOWER_MULT       = 0.8
	RC_CAP_UPPER_MULT       = 1.75
	RC_MIN_RATE             = 10.0
)

// --- Asynchronous Workload Settings ---
const (
	WORKLOAD_TYPE        = "asynchronous" // "synchronous" or "asynchronous"
	SYNCHRONIZATION_TIME = 1001500        // microseconds
)

type ConsumerTag struct{}

func (ConsumerTag) String() string { return "NDN-Consumer" }

var consumerTag = ConsumerTag{}

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

// --- Packet Structures ---

type PacketSample struct {
	Timestamp time.Time
	Size      int
	RTT       float64
}

type RTTSample struct {
	Timestamp time.Time
	RTT       float64
}

type FlowContext struct {
	mu       sync.Mutex
	Prefix   string
	baseName enc.Name
	Mode     TransmissionMode

	// --- Shared State ---
	currentRate  float64
	rto          float64
	pendingSends map[string]time.Time

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
	pktsCalibrated        int
	initialRTTSum         float64
	baseRTT               float64
	adjustmentPeriod      time.Duration
	lastRateUpdate        time.Time
	interestsSentInPeriod int

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
		pendingSends: make(map[string]time.Time),
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
	f.currentRate = DT_INIT_RATE
	f.rto = MIN_RTO * 2

	f.rttHistory = make([]RTTSample, 0)
	f.estimatedBandwidth = DT_INIT_RATE
	f.window = make([]PacketSample, 0)
	f.pktsCalibrated = 0
	f.lastRateUpdate = time.Now()

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

func (f *FlowContext) RecordInterestSend(name string, t time.Time) {
	f.mu.Lock()
	f.pendingSends[name] = t

	if f.Mode == ModeDT {
		f.interestsSentInPeriod++
	} else {
		f.OutstandingPD++
	}
	f.mu.Unlock()
}

// --- Core Event Handlers ---

// OnNack handles Nack packets.
func (f *FlowContext) OnNack(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// TODO: added by yitong, nack pipeline
	// Simple printf for debugging
	fmt.Printf("[NACK DEBUG] Received NACK for: %s, Mode: %v, Prefix: %s\n", name, f.Mode, f.Prefix)

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

// OnData handles Data packets.
func (f *FlowContext) OnData(payload []byte, dataName string, receiveTime time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()

	sendTime, ok := f.pendingSends[dataName]
	if !ok {
		return
	}
	delete(f.pendingSends, dataName)

	if f.isFinished {
		return
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
			return
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
		return
	}

	// =========================================================
	//             MODE B: DATA TRANSMISSION (DT)
	// =========================================================
	if f.Mode == ModeDT {
		_, err := datapacket.DeserializeData(payload)
		if err != nil {
			log.Warn(consumerTag, "DT: Failed to deserialize packet", "err", err)
			return
		}

		rttVal := float64(receiveTime.Sub(sendTime).Nanoseconds()) / 1e6

		f.endTime = receiveTime
		f.totalBytes += int64(len(payload))
		f.pktsReceivedTotal++

		if f.pktsReceivedTotal >= MAX_PACKETS {
			f.isFinished = true
			close(f.doneCh)
		}

		seqNum := extractSeqNum(dataName)
		if seqNum >= 0 && seqNum%200 == 0 {
			log.Debug(consumerTag, "DT Data Received", "name", dataName, "rtt", rttVal, "totalRx", f.pktsReceivedTotal)
		}

		f.updateRTT(receiveTime, rttVal)
		f.updateRateControl(receiveTime, len(payload), rttVal)
	}
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
		finalRTO = 3 * calculatedRTO
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
		finalRTO = 3 * calculatedRTO
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

func (f *FlowContext) updateRateControl(receiveTime time.Time, payloadSize int, rttVal float64) {
	f.window = append(f.window, PacketSample{Timestamp: receiveTime, Size: payloadSize, RTT: rttVal})
	cutoff := receiveTime.Add(-THROUGHPUT_WINDOW_DUR)
	validWinIdx := 0
	for i, s := range f.window {
		if s.Timestamp.After(cutoff) {
			validWinIdx = i
			break
		}
	}
	f.window = f.window[validWinIdx:]

	if f.pktsCalibrated < 5 {
		f.initialRTTSum += rttVal
		f.pktsCalibrated++
		if f.pktsCalibrated == 5 {
			f.baseRTT = f.initialRTTSum / 5.0
			f.adjustmentPeriod = time.Duration(f.initialRTTSum) * time.Millisecond
			if f.adjustmentPeriod < 20*time.Millisecond {
				f.adjustmentPeriod = 20 * time.Millisecond
			}
			f.lastRateUpdate = receiveTime
			f.interestsSentInPeriod = 0
			log.Info(consumerTag, "Flow Calibration Complete", "flow", f.Prefix, "baseRTT", f.baseRTT)
		}
		return
	}

	timeSinceLastUpdate := receiveTime.Sub(f.lastRateUpdate)
	if timeSinceLastUpdate >= f.adjustmentPeriod {
		actualRate := 0.0
		if timeSinceLastUpdate.Seconds() > 0 {
			actualRate = float64(f.interestsSentInPeriod) / timeSinceLastUpdate.Seconds()
		}

		var totalWinRTT float64
		wCount := len(f.window)
		if wCount == 0 {
			return
		}
		for _, s := range f.window {
			totalWinRTT += s.RTT
		}
		avgWinRTT := totalWinRTT / float64(wCount)
		measuredPPS := float64(wCount) / THROUGHPUT_WINDOW_DUR.Seconds()

		if measuredPPS > f.estimatedBandwidth {
			f.estimatedBandwidth = measuredPPS
		}
		if avgWinRTT > RC_RTT_LOW_THRESH_MULT*f.baseRTT {
			f.estimatedBandwidth = measuredPPS
		}

		targetRate := f.currentRate
		if avgWinRTT < RC_RTT_LOW_THRESH_MULT*f.baseRTT {
			targetRate = f.currentRate * RC_INC_FACTOR
		} else if avgWinRTT >= RC_RTT_LOW_THRESH_MULT*f.baseRTT && avgWinRTT < RC_RTT_HIGH_THRESH_MULT*f.baseRTT {
			targetRate = f.currentRate
		} else {
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

		log.Debug(consumerTag, "Rate Adjusted",
			"prefix", f.Prefix,
			"oldTargetRate", oldRate,
			"newTargetRate", f.currentRate,
			"actualRate", actualRate,
			"estimatedBW", f.estimatedBandwidth,
			"baseRTT", f.baseRTT,
			"latestRTT", avgWinRTT)

		f.interestsSentInPeriod = 0
	}
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
						log.Debug(consumerTag, "DT: RTO Expired - Retransmitting", "name", item.Name)

						// Double RTO (Backoff) or just use current RTO?
						// Simple implementation: Use current updated RTO from flow
						rto := item.FlowCtx.GetRTO()
						item.Expiration = now.Add(time.Duration(rto) * time.Millisecond)

						// Re-queue
						heap.Push(&rc.pq, item)
						rc.mu.Unlock()

						rc.stats.IncrementRetx()
						// Trigger retransmission
						go sendSingleInterest(rc.app, rc.stats, item.SequenceNum, item.FlowCtx, true)
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
	flowCtx.RecordInterestSend(interestName, sendTime)

	// Add to Controller (if it's a retx, the controller already popped it, so we add it back/update it via Add)
	// Note: In StartScheduler for DT, we pushed it back to heap. But here we might be adding a duplicate if we aren't careful.
	// However, StartScheduler logic for DT pushes *item* back.
	// If we call Add() here, we create a *new* item.
	// Correction: StartScheduler pushes the *existing* item back to wait for the *next* RTO.
	// We do NOT need to call Add() again if StartScheduler already re-queued it.
	// BUT: StartScheduler calls sendSingleInterest with isRetransmission=true.
	// If we don't call Add(), the map `active` is fine (it wasn't removed).
	// So: Only call Add if NOT retransmission.
	if !isRetransmission {
		globalRetxController.Add(interestName, flowCtx.Prefix, sequenceNum, flowCtx)
	}

	err = app.Express(interest,
		func(args ndn.ExpressCallbackArgs) {
			receiveTime := time.Now()
			switch args.Result {
			case ndn.InterestResultNack:
				stats.IncrementNack()
				globalRetxController.Remove(interestName)
				flowCtx.OnNack(interestName)

			case ndn.InterestResultTimeout:
				// This is the NDN Framework Timeout (Lifetime ~4s).
				// The Application RTO (RetransmissionController) handles the "real" logic.
				// We just log this for debugging "dead" packets.
				log.Debug(consumerTag, "NDN Framework Timeout (Lifetime reached)", "name", interestName)
				// We should ensure the controller stops tracking this if it hasn't already.
				globalRetxController.Remove(interestName)
				// We do NOT call flowCtx.OnTimeout here, because the Controller likely already did
				// (or will do) the RTO logic.

			case ndn.InterestCancelled:
				globalRetxController.Remove(interestName)

			case ndn.InterestResultData:
				data := args.Data
				content := data.Content().Join()
				dataName := data.Name().String()
				globalRetxController.Remove(dataName)
				stats.IncrementData()
				flowCtx.OnData(content, dataName, receiveTime)
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
	const MAX_BURST = 5.0

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
			if tokens > MAX_BURST {
				tokens = MAX_BURST
			}

			for tokens >= 1.0 {
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

	targetPrefixes, err := parseProducerRange(rangeStr)
	if err != nil {
		log.Fatal(consumerTag, "Failed to parse producer range", "err", err)
		return
	}

	// log.Default().SetLevel(log.LevelDebug)
	log.Default().SetLevel(log.LevelInfo)

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

// package main

// import (
// 	"container/heap"
// 	"fmt"
// 	"math"
// 	"os"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"time"

// 	enc "github.com/named-data/ndnd/std/encoding"
// 	"github.com/named-data/ndnd/std/engine"
// 	"github.com/named-data/ndnd/std/log"
// 	"github.com/named-data/ndnd/std/ndn"
// 	"github.com/named-data/ndnd/std/types/optional"
// 	"github.com/named-data/ndnd/std/utils"
// )

// // --- Configuration Constants ---

// // TODO: 50Mbps setup
// const (
// 	INIT_INTEREST_RATE    = 3000.0                // Initial Rate: interests/sec
// 	TOTAL_DURATION        = 30 * time.Second      // Max duration
// 	MAX_PACKETS           = 15000                 // Target packets per flow
// 	INTEREST_LIFETIME     = 4 * time.Second       // Interest lifetime
// 	RTT_WINDOW_DURATION   = 1 * time.Second       // RTT sample window
// 	MIN_RTO               = 60.0                  // Min RTO (ms)
// 	MAX_RTO               = 600.0                 // Max RTO (ms)
// 	THROUGHPUT_WINDOW_DUR = 20 * time.Millisecond // Sliding window size
// 	TICKER_INTERVAL       = 1 * time.Millisecond  // Granularity for Token Bucket
// )

// // --- Rate Control Hyperparameters (Global) ---
// var (
// 	RC_RTT_LOW_THRESH_MULT  = 3.0  // Multiplier for BaseRTT (Low Threshold)
// 	RC_RTT_HIGH_THRESH_MULT = 6.0  // Multiplier for BaseRTT (High Threshold)
// 	RC_INC_FACTOR           = 1.05 // Rate increase factor
// 	RC_DEC_FACTOR           = 0.95 // Rate decrease factor
// 	RC_CAP_LOWER_MULT       = 0.8  // Lower cap multiplier (vs Bandwidth)
// 	RC_CAP_UPPER_MULT       = 1.75 // Upper cap multiplier (vs Bandwidth)
// 	RC_MIN_RATE             = 10.0 // Absolute minimum rate
// )

// // Create a tag for our consumer application
// type ConsumerTag struct{}

// func (ConsumerTag) String() string { return "NDN-Consumer" }

// var consumerTag = ConsumerTag{}

// type TransmissionMode int

// const (
// 	ModePD TransmissionMode = iota
// 	ModeDT
// )

// var selectedMode = ModeDT
// var consumerNodeName string

// // PacketSample for throughput calculation
// type PacketSample struct {
// 	Timestamp time.Time
// 	Size      int
// 	RTT       float64
// }

// // RTTSample for RTO calculation
// type RTTSample struct {
// 	Timestamp time.Time
// 	RTT       float64
// }

// // --- OPTIMIZATION C: Decentralized RTT Tracking ---
// // RTT logic is moved inside FlowContext to avoid global lock contention.

// type FlowContext struct {
// 	mu     sync.Mutex
// 	Prefix string

// 	// Pre-computed base name components to save allocations (Optimization D)
// 	baseName enc.Name

// 	// Path Discovery
// 	PdIndex    int
// 	PdFinished bool

// 	// General Stats
// 	totalBytes        int64
// 	pktsReceivedTotal int
// 	startTime         time.Time
// 	endTime           time.Time
// 	isFinished        bool
// 	doneCh            chan struct{}

// 	// Rate Control / Throughput Window
// 	window             []PacketSample
// 	currentRate        float64
// 	estimatedBandwidth float64 // NEW: Bandwidth estimation
// 	pktsCalibrated     int
// 	initialRTTSum      float64
// 	baseRTT            float64
// 	adjustmentPeriod   time.Duration
// 	lastRateUpdate     time.Time

// 	// New Field: Track actual sends for interval logging
// 	interestsSentInPeriod int

// 	// --- RTT / RTO State (Per-Flow) ---
// 	rttHistory []RTTSample
// 	srtt       float64
// 	rttVar     float64
// 	rto        float64

// 	// Map to track send times for RTT calculation within this flow
// 	pendingSends map[string]time.Time
// }

// func NewFlowContext(prefix string) *FlowContext {
// 	now := time.Now()

// 	// Pre-compute base name: /<prefix>/<node>/[pd/0 | dt]
// 	// This avoids parsing the prefix string for every packet.
// 	n, _ := enc.NameFromStr(prefix)
// 	if consumerNodeName != "" {
// 		n = n.Append(enc.NewStringComponent(0x08, consumerNodeName))
// 	}

// 	if selectedMode == ModePD {
// 		n = n.Append(enc.NewStringComponent(0x08, "pd"))
// 		n = n.Append(enc.NewStringComponent(0x08, "0")) // Default index 0
// 	} else {
// 		n = n.Append(enc.NewStringComponent(0x08, "dt"))
// 	}

// 	return &FlowContext{
// 		Prefix:                prefix,
// 		baseName:              n,
// 		PdIndex:               0,
// 		startTime:             now,
// 		endTime:               now,
// 		window:                make([]PacketSample, 0),
// 		currentRate:           INIT_INTEREST_RATE,
// 		estimatedBandwidth:    INIT_INTEREST_RATE, // Initialize to initial rate to avoid zero caps
// 		pktsCalibrated:        0,
// 		lastRateUpdate:        now,
// 		doneCh:                make(chan struct{}),
// 		interestsSentInPeriod: 0,

// 		// RTT Init
// 		rttHistory:   make([]RTTSample, 0),
// 		srtt:         0,
// 		rttVar:       0,
// 		rto:          MIN_RTO * 2,
// 		pendingSends: make(map[string]time.Time),
// 	}
// }

// func (f *FlowContext) GetRate() float64 {
// 	f.mu.Lock()
// 	defer f.mu.Unlock()
// 	return f.currentRate
// }

// func (f *FlowContext) GetRTO() float64 {
// 	f.mu.Lock()
// 	defer f.mu.Unlock()
// 	return f.rto
// }

// // RecordInterestSend stores timestamp for RTT calc
// func (f *FlowContext) RecordInterestSend(name string, t time.Time) {
// 	f.mu.Lock()
// 	f.pendingSends[name] = t
// 	// Track actual sends for the interval log
// 	f.interestsSentInPeriod++
// 	f.mu.Unlock()
// }

// // UpdateBandwidth updates the bandwidth estimation based on throughput and RTT
// func (f *FlowContext) UpdateBandwidth(currentThroughput float64, currentRTT float64) {
// 	// Rule 1: If latest throughput measurement is larger than current bandwidth estimation, update it.
// 	if currentThroughput > f.estimatedBandwidth {
// 		f.estimatedBandwidth = currentThroughput
// 	}

// 	// Rule 2: If latest estimated rtt is larger than 3*base_rtt (Congestion), update bandwidth as latest throughput.
// 	// This effectively "drains" the bandwidth estimate when congestion is detected.
// 	if currentRTT > RC_RTT_LOW_THRESH_MULT*f.baseRTT {
// 		f.estimatedBandwidth = currentThroughput
// 	}
// }

// // RecordPacket handles both RTT calculation and Rate Adaptation
// func (f *FlowContext) RecordPacket(payloadSize int, dataName string, receiveTime time.Time) {
// 	f.mu.Lock()
// 	defer f.mu.Unlock()

// 	// --- 1. RTT Calculation Logic (Moved from global tracker) ---
// 	sendTime, ok := f.pendingSends[dataName]
// 	if !ok {
// 		// Might be a duplicate or timed out already
// 		return
// 	}
// 	delete(f.pendingSends, dataName) // Clean up

// 	rttVal := float64(receiveTime.Sub(sendTime).Nanoseconds()) / 1e6

// 	// Log data received every 200 packets (per prefix)
// 	seqNum := extractSeqNum(dataName)
// 	if seqNum >= 0 && seqNum%200 == 0 {
// 		log.Debug(consumerTag, "Data Received",
// 			"name", dataName,
// 			"rtt", rttVal)
// 	}

// 	// Update RTT History Window
// 	f.rttHistory = append(f.rttHistory, RTTSample{Timestamp: receiveTime, RTT: rttVal})

// 	// Prune old RTT samples
// 	validIdx := 0
// 	for i, s := range f.rttHistory {
// 		if receiveTime.Sub(s.Timestamp) <= RTT_WINDOW_DURATION {
// 			validIdx = i
// 			break
// 		}
// 	}
// 	f.rttHistory = f.rttHistory[validIdx:]

// 	// Calculate RTO (Equations)
// 	count := len(f.rttHistory)
// 	var meanRTT, variance, calculatedRTO, finalRTO float64

// 	if count < 2 {
// 		meanRTT = rttVal
// 		variance = 0.0
// 		calculatedRTO = math.Max(MIN_RTO, 2*rttVal)
// 		finalRTO = 3 * calculatedRTO
// 	} else {
// 		var sum float64
// 		for _, s := range f.rttHistory {
// 			sum += s.RTT
// 		}
// 		meanRTT = sum / float64(count)

// 		var varSum float64
// 		for _, s := range f.rttHistory {
// 			diff := s.RTT - meanRTT
// 			varSum += diff * diff
// 		}
// 		variance = varSum / float64(count)

// 		stdDev := math.Sqrt(variance)
// 		calculatedRTO = meanRTT + 4*stdDev
// 		finalRTO = 3 * calculatedRTO
// 	}

// 	if finalRTO < MIN_RTO {
// 		finalRTO = MIN_RTO
// 	}
// 	if finalRTO > MAX_RTO {
// 		finalRTO = MAX_RTO
// 	}

// 	f.srtt = meanRTT
// 	f.rttVar = variance
// 	f.rto = finalRTO

// 	// --- 2. Stats & Rate Control ---

// 	if f.isFinished {
// 		return
// 	}

// 	f.endTime = receiveTime
// 	f.totalBytes += int64(payloadSize)
// 	f.pktsReceivedTotal++

// 	if f.pktsReceivedTotal >= MAX_PACKETS {
// 		f.isFinished = true
// 		close(f.doneCh)
// 	}

// 	// Update Throughput Window
// 	f.window = append(f.window, PacketSample{Timestamp: receiveTime, Size: payloadSize, RTT: rttVal})
// 	cutoff := receiveTime.Add(-THROUGHPUT_WINDOW_DUR)
// 	validWinIdx := 0
// 	for i, s := range f.window {
// 		if s.Timestamp.After(cutoff) {
// 			validWinIdx = i
// 			break
// 		}
// 	}
// 	f.window = f.window[validWinIdx:]

// 	// Calibration & Dynamic Rate (Same logic as original, just integrated)
// 	if f.pktsCalibrated < 5 {
// 		f.initialRTTSum += rttVal
// 		f.pktsCalibrated++
// 		if f.pktsCalibrated == 5 {
// 			f.baseRTT = f.initialRTTSum / 5.0
// 			f.adjustmentPeriod = time.Duration(f.initialRTTSum) * time.Millisecond
// 			if f.adjustmentPeriod < 20*time.Millisecond {
// 				f.adjustmentPeriod = 20 * time.Millisecond
// 			}
// 			f.lastRateUpdate = receiveTime
// 			// Reset counter for the first real interval
// 			f.interestsSentInPeriod = 0
// 			log.Info(consumerTag, "Flow Calibration Complete", "flow", f.Prefix, "baseRTT", f.baseRTT)
// 		}
// 		return
// 	}

// 	timeSinceLastUpdate := receiveTime.Sub(f.lastRateUpdate)

// 	if timeSinceLastUpdate >= f.adjustmentPeriod {
// 		// --- LOGGING UPDATE: Calculate Actual Rate vs Target Rate ---
// 		actualRate := 0.0
// 		if timeSinceLastUpdate.Seconds() > 0 {
// 			actualRate = float64(f.interestsSentInPeriod) / timeSinceLastUpdate.Seconds()
// 		}

// 		// Calculate Measured Throughput (PPS)
// 		var totalWinRTT float64
// 		wCount := len(f.window)
// 		if wCount == 0 {
// 			return
// 		}

// 		for _, s := range f.window {
// 			totalWinRTT += s.RTT
// 		}
// 		avgWinRTT := totalWinRTT / float64(wCount)
// 		measuredPPS := float64(wCount) / THROUGHPUT_WINDOW_DUR.Seconds()

// 		// Update Bandwidth Estimation
// 		f.UpdateBandwidth(measuredPPS, avgWinRTT)

// 		// Rate Adjustment Logic
// 		targetRate := f.currentRate
// 		if avgWinRTT < RC_RTT_LOW_THRESH_MULT*f.baseRTT {
// 			targetRate = f.currentRate * RC_INC_FACTOR
// 		} else if avgWinRTT >= RC_RTT_LOW_THRESH_MULT*f.baseRTT && avgWinRTT < RC_RTT_HIGH_THRESH_MULT*f.baseRTT {
// 			targetRate = f.currentRate // Hold
// 		} else {
// 			targetRate = f.estimatedBandwidth * RC_DEC_FACTOR
// 		}

// 		// Caps based on Bandwidth (instead of measuredPPS)
// 		lower := RC_CAP_LOWER_MULT * f.estimatedBandwidth
// 		if lower < 1.0 {
// 			lower = 1.0
// 		}
// 		upper := RC_CAP_UPPER_MULT * f.estimatedBandwidth
// 		if upper < INIT_INTEREST_RATE {
// 			upper = INIT_INTEREST_RATE
// 		}

// 		if targetRate < lower {
// 			targetRate = lower
// 		}
// 		if targetRate > upper {
// 			targetRate = upper
// 		}
// 		if targetRate < RC_MIN_RATE {
// 			targetRate = RC_MIN_RATE
// 		}

// 		oldRate := f.currentRate
// 		f.currentRate = targetRate
// 		f.lastRateUpdate = receiveTime

// 		// Log rate adjustment
// 		log.Debug(consumerTag, "Rate Adjusted",
// 			"prefix", f.Prefix,
// 			"oldTargetRate", oldRate,
// 			"newTargetRate", f.currentRate,
// 			"actualRate", actualRate,
// 			"estimatedBW", f.estimatedBandwidth,
// 			"baseRTT", f.baseRTT,
// 			"latestRTT", avgWinRTT)

// 		// Reset the counter for the next interval
// 		f.interestsSentInPeriod = 0
// 	}
// }

// func (f *FlowContext) GetFinalStats() (float64, int64, float64) {
// 	f.mu.Lock()
// 	defer f.mu.Unlock()
// 	duration := f.endTime.Sub(f.startTime).Seconds()
// 	if duration <= 0 {
// 		return 0.0, f.totalBytes, 0.0
// 	}
// 	throughputMbps := (float64(f.totalBytes) * 8.0) / (duration * 1e6)
// 	return throughputMbps, f.totalBytes, duration
// }

// // --- Statistics ---

// type Statistics struct {
// 	mu              sync.Mutex
// 	totalSent       int
// 	dataReceived    int
// 	nackReceived    int
// 	timeoutReceived int
// 	retransmissions int
// }

// func (s *Statistics) IncrementSent()    { s.mu.Lock(); s.totalSent++; s.mu.Unlock() }
// func (s *Statistics) IncrementData()    { s.mu.Lock(); s.dataReceived++; s.mu.Unlock() }
// func (s *Statistics) IncrementNack()    { s.mu.Lock(); s.nackReceived++; s.mu.Unlock() }
// func (s *Statistics) IncrementTimeout() { s.mu.Lock(); s.timeoutReceived++; s.mu.Unlock() }
// func (s *Statistics) IncrementRetx()    { s.mu.Lock(); s.retransmissions++; s.mu.Unlock() }

// func (s *Statistics) Print() {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()
// 	log.Info(consumerTag, "=== Final Statistics ===",
// 		"totalSent", s.totalSent,
// 		"dataReceived", s.dataReceived,
// 		"nackReceived", s.nackReceived,
// 		"timeouts", s.timeoutReceived,
// 		"retransmissions", s.retransmissions)
// }

// // --- OPTIMIZATION E: Heap-based Retransmission Controller ---

// type PendingPacket struct {
// 	Name        string
// 	Prefix      string
// 	Expiration  time.Time // Key for Heap
// 	SequenceNum int
// 	FlowCtx     *FlowContext
// 	Index       int // For heap interface
// }

// // PacketHeap implements heap.Interface
// type PacketHeap []*PendingPacket

// func (h PacketHeap) Len() int           { return len(h) }
// func (h PacketHeap) Less(i, j int) bool { return h[i].Expiration.Before(h[j].Expiration) }
// func (h PacketHeap) Swap(i, j int) {
// 	h[i], h[j] = h[j], h[i]
// 	h[i].Index = i
// 	h[j].Index = j
// }
// func (h *PacketHeap) Push(x interface{}) {
// 	n := len(*h)
// 	item := x.(*PendingPacket)
// 	item.Index = n
// 	*h = append(*h, item)
// }
// func (h *PacketHeap) Pop() interface{} {
// 	old := *h
// 	n := len(old)
// 	item := old[n-1]
// 	old[n-1] = nil // Avoid memory leak
// 	item.Index = -1
// 	*h = old[0 : n-1]
// 	return item
// }

// type RetransmissionController struct {
// 	mu sync.Mutex
// 	pq PacketHeap
// 	// Map to track status. If missing from map, it means it was acked/cancelled.
// 	// If present, it maps to the SequenceNum to verify validity (in case of seq wrapping, though unlikely here)
// 	active   map[string]int
// 	app      ndn.Engine
// 	stats    *Statistics
// 	stopCh   chan struct{}
// 	wakeupCh chan struct{} // To wake up the scheduler loop when new item added
// }

// func NewRetransmissionController(app ndn.Engine, stats *Statistics) *RetransmissionController {
// 	rc := &RetransmissionController{
// 		pq:       make(PacketHeap, 0),
// 		active:   make(map[string]int),
// 		app:      app,
// 		stats:    stats,
// 		stopCh:   make(chan struct{}),
// 		wakeupCh: make(chan struct{}, 1),
// 	}
// 	heap.Init(&rc.pq)
// 	return rc
// }

// func (rc *RetransmissionController) Add(name string, prefix string, seq int, flowCtx *FlowContext) {
// 	rc.mu.Lock()
// 	defer rc.mu.Unlock()

// 	// Calculate Expiration based on current Flow RTO
// 	rto := flowCtx.GetRTO()
// 	expiration := time.Now().Add(time.Duration(rto) * time.Millisecond)

// 	pkt := &PendingPacket{
// 		Name:        name,
// 		Prefix:      prefix,
// 		Expiration:  expiration,
// 		SequenceNum: seq,
// 		FlowCtx:     flowCtx,
// 	}

// 	rc.active[name] = seq
// 	heap.Push(&rc.pq, pkt)

// 	// Non-blocking signal to wake up loop if this is the new earliest item
// 	select {
// 	case rc.wakeupCh <- struct{}{}:
// 	default:
// 	}
// }

// func (rc *RetransmissionController) Remove(name string) {
// 	rc.mu.Lock()
// 	defer rc.mu.Unlock()
// 	// Lazy removal: just delete from map.
// 	// When the heap pops the item, we check the map. If missing, we ignore.
// 	delete(rc.active, name)
// }

// func (rc *RetransmissionController) StartScheduler() {
// 	for {
// 		var waitDuration time.Duration

// 		rc.mu.Lock()
// 		if rc.pq.Len() == 0 {
// 			waitDuration = 10 * time.Second // Idle wait
// 		} else {
// 			now := time.Now()
// 			top := rc.pq[0]
// 			if now.After(top.Expiration) {
// 				// Expired! Pop and Process
// 				item := heap.Pop(&rc.pq).(*PendingPacket)

// 				// Check if still active
// 				if seq, exists := rc.active[item.Name]; exists && seq == item.SequenceNum {
// 					// Needs Retransmission
// 					// Re-add to heap with NEW expiration
// 					rto := item.FlowCtx.GetRTO()
// 					item.Expiration = now.Add(time.Duration(rto) * time.Millisecond)
// 					heap.Push(&rc.pq, item)

// 					// Unlock before sending to avoid blocking
// 					rc.mu.Unlock()

// 					rc.stats.IncrementRetx()
// 					// Synchronous send (on a separate goroutine is fine here as Retx is low freq compared to main flow)
// 					// But to keep it lightweight, we just fire it.
// 					go sendSingleInterest(rc.app, rc.stats, item.SequenceNum, item.FlowCtx, true)
// 					continue // Loop again immediately
// 				}
// 				// Else: was removed/acked, ignore.
// 				rc.mu.Unlock()
// 				continue
// 			} else {
// 				// Not expired yet
// 				waitDuration = top.Expiration.Sub(now)
// 			}
// 		}
// 		rc.mu.Unlock()

// 		select {
// 		case <-rc.stopCh:
// 			return
// 		case <-rc.wakeupCh:
// 			// New item added, re-evaluate heap
// 			continue
// 		case <-time.After(waitDuration):
// 			// Timer expired, check heap
// 			continue
// 		}
// 	}
// }

// func (rc *RetransmissionController) Stop() {
// 	close(rc.stopCh)
// }

// var globalRetxController *RetransmissionController

// // --- Sending Logic ---

// // sendSingleInterest is now optimized for allocations and called synchronously
// func sendSingleInterest(app ndn.Engine, stats *Statistics, sequenceNum int, flowCtx *FlowContext, isRetransmission bool) {
// 	// OPTIMIZATION D: Allocation Optimization
// 	// Use pre-computed baseName. Append sequence number efficiently.
// 	// Note: We need a copy because Append modifies the slice.
// 	// Since enc.Name is essentially a slice of components, shallow copy is cheap,
// 	// but appending requires new backing array usually.

// 	// Create the sequence component string manually or efficiently
// 	// Using strconv.AppendInt to a buffer to avoid fmt.Sprintf
// 	// For simplicity in NDN lib usage, we create the component directly.

// 	// We reconstruct name from base:
// 	name := flowCtx.baseName.Append(enc.NewStringComponent(0x08, "seq-"+strconv.Itoa(sequenceNum)))

// 	intCfg := &ndn.InterestConfig{
// 		MustBeFresh: true,
// 		Lifetime:    optional.Some(INTEREST_LIFETIME),
// 		Nonce:       utils.ConvertNonce(app.Timer().Nonce()),
// 	}

// 	interest, err := app.Spec().MakeInterest(name, intCfg, nil, nil)
// 	if err != nil {
// 		return
// 	}

// 	stats.IncrementSent()
// 	interestName := interest.FinalName.String()
// 	sendTime := time.Now()

// 	// Log interest every 200 packets with current send rate
// 	if sequenceNum%200 == 0 {
// 		currentRate := flowCtx.GetRate()
// 		log.Debug(consumerTag, "Interest Sent",
// 			"name", interestName,
// 			"rate", currentRate)
// 	}

// 	// Record send time for RTT (Local Flow Context)
// 	flowCtx.RecordInterestSend(interestName, sendTime)

// 	// Add to Heap Controller
// 	globalRetxController.Add(interestName, flowCtx.Prefix, sequenceNum, flowCtx)

// 	err = app.Express(interest,
// 		func(args ndn.ExpressCallbackArgs) {
// 			receiveTime := time.Now()
// 			switch args.Result {
// 			case ndn.InterestResultNack:
// 				stats.IncrementNack()
// 				globalRetxController.Remove(interestName)
// 			case ndn.InterestResultTimeout:
// 				stats.IncrementTimeout()
// 				globalRetxController.Remove(interestName)
// 			case ndn.InterestCancelled:
// 				// stats.IncrementCancel()
// 				globalRetxController.Remove(interestName)
// 			case ndn.InterestResultData:
// 				data := args.Data
// 				content := data.Content().Join()
// 				dataName := data.Name().String()

// 				// Mark as received in Retx Controller (Lazy removal)
// 				globalRetxController.Remove(dataName)

// 				// Update Flow Stats & Rate (Decentralized)
// 				flowCtx.RecordPacket(len(content), dataName, receiveTime)

// 				stats.IncrementData()
// 			}
// 		})
// }

// // --- Main Flow Loop ---

// func runFlow(app ndn.Engine, stats *Statistics, flowCtx *FlowContext, wg *sync.WaitGroup) {
// 	defer wg.Done()

// 	totalTimer := time.NewTimer(TOTAL_DURATION)
// 	defer totalTimer.Stop()

// 	// OPTIMIZATION B: Token Bucket Pacing
// 	// Instead of sleep(1/rate), we accumulate tokens.
// 	ticker := time.NewTicker(TICKER_INTERVAL)
// 	defer ticker.Stop()

// 	tokens := 0.0
// 	sequenceNum := 0

// 	// Max burst allows sending a few packets back-to-back if we fell behind slightly,
// 	// but prevents dumping huge spikes.
// 	const MAX_BURST = 5.0

// 	log.Info(consumerTag, "Starting flow", "target", flowCtx.Prefix)

// 	for {
// 		select {
// 		case <-totalTimer.C:
// 			return
// 		case <-flowCtx.doneCh:
// 			return
// 		case <-ticker.C:
// 			// Refill tokens based on current rate
// 			// Rate is packets/sec. Ticker is TICKER_INTERVAL.
// 			// tokens += rate * interval_in_seconds
// 			currentRate := flowCtx.GetRate()
// 			tokens += currentRate * TICKER_INTERVAL.Seconds()

// 			if tokens > MAX_BURST {
// 				tokens = MAX_BURST
// 			}

// 			// OPTIMIZATION A: Remove Per-Packet Goroutines
// 			// We loop here and send synchronously while we have tokens.
// 			// This keeps the goroutine count constant (N flows = N goroutines).
// 			for tokens >= 1.0 {
// 				if sequenceNum >= MAX_PACKETS {
// 					// Wait for completion or timeout
// 					tokens = 0
// 					break
// 				}

// 				sequenceNum++
// 				tokens -= 1.0

// 				// Direct call, no 'go' keyword
// 				sendSingleInterest(app, stats, sequenceNum, flowCtx, false)
// 			}
// 		}
// 	}
// }

// func main() {
// 	if len(os.Args) < 3 {
// 		fmt.Fprintf(os.Stderr, "Usage: %s <node_name> <producer_range>\n", os.Args[0])
// 		os.Exit(1)
// 	}

// 	nodeName := os.Args[1]
// 	rangeStr := os.Args[2]
// 	consumerNodeName = nodeName

// 	targetPrefixes, err := parseProducerRange(rangeStr)
// 	if err != nil {
// 		log.Fatal(consumerTag, "Failed to parse producer range", "err", err)
// 		return
// 	}

// 	log.Default().SetLevel(log.LevelDebug)
// 	// log.Default().SetLevel(log.LevelInfo)
// 	os.Setenv("NDN_CLIENT_TRANSPORT", "tcp://127.0.0.1:6363")

// 	app := engine.NewBasicEngine(engine.NewDefaultFace())
// 	err = app.Start()
// 	if err != nil {
// 		log.Fatal(consumerTag, "Unable to start engine", "err", err)
// 		return
// 	}
// 	defer app.Stop()

// 	stats := &Statistics{}

// 	// Initialize Heap-based Retransmission Controller
// 	globalRetxController = NewRetransmissionController(app, stats)
// 	go globalRetxController.StartScheduler()
// 	defer globalRetxController.Stop()

// 	log.Info(consumerTag, "Starting Optimized NDN Consumer",
// 		"nodeName", nodeName,
// 		"targets", len(targetPrefixes),
// 		"mode", "TokenBucket+HeapRetx")

// 	var wg sync.WaitGroup
// 	startTime := time.Now()
// 	flowContexts := make([]*FlowContext, 0, len(targetPrefixes))

// 	for _, prefix := range targetPrefixes {
// 		wg.Add(1)
// 		flowCtx := NewFlowContext(prefix)
// 		flowContexts = append(flowContexts, flowCtx)
// 		go runFlow(app, stats, flowCtx, &wg)
// 	}

// 	wg.Wait()
// 	elapsed := time.Since(startTime)
// 	log.Info(consumerTag, "All flows stopped", "elapsedTime", elapsed)

// 	time.Sleep(1 * time.Second)
// 	stats.Print()

// 	log.Info(consumerTag, "=== Final Per-Flow Statistics ===")
// 	for _, flowCtx := range flowContexts {
// 		avgThroughput, totalBytes, duration := flowCtx.GetFinalStats()
// 		log.Info(consumerTag, "Flow Summary",
// 			"flow", flowCtx.Prefix,
// 			"totalBytes", totalBytes,
// 			"avgMbps", avgThroughput,
// 			"duration", duration)
// 	}
// }

// // Helpers

// // extractSeqNum extracts the sequence number from a name string.
// // Expected format: /prefix/node/[pd/<index>/|dt/]seq-<num>
// // Returns -1 if parsing fails.
// func extractSeqNum(nameStr string) int {
// 	parts := strings.Split(nameStr, "/")
// 	if len(parts) == 0 {
// 		return -1
// 	}
// 	// Last component should be "seq-N"
// 	lastPart := parts[len(parts)-1]
// 	if !strings.HasPrefix(lastPart, "seq-") {
// 		return -1
// 	}
// 	seqStr := strings.TrimPrefix(lastPart, "seq-")
// 	seqNum, err := strconv.Atoi(seqStr)
// 	if err != nil {
// 		return -1
// 	}
// 	return seqNum
// }

// func parseProducerRange(rangeStr string) ([]string, error) {
// 	parts := strings.Split(rangeStr, "-")
// 	if len(parts) != 2 {
// 		return nil, fmt.Errorf("invalid format")
// 	}
// 	start, err1 := strconv.Atoi(parts[0])
// 	end, err2 := strconv.Atoi(parts[1])
// 	if err1 != nil || err2 != nil {
// 		return nil, fmt.Errorf("invalid numbers")
// 	}
// 	var prefixes []string
// 	for i := start; i <= end; i++ {
// 		prefixes = append(prefixes, fmt.Sprintf("/pro%dapp", i))
// 	}
// 	return prefixes, nil
// }
