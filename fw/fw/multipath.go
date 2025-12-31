package fw

import (
	"bytes"
	"sort"
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
}

// Multipath Strategy Struct
type Multipath struct {
	StrategyBase
	pdState sync.Map
}

func init() {
	strategyInit = append(strategyInit, func() Strategy { return &Multipath{} })
	StrategyVersions["multipath"] = []uint64{1}
}

func (s *Multipath) Instantiate(fwThread *Thread) {
	s.NewStrategyBase(fwThread, "multipath", 1)
}

// Helper to get or create the state for a prefix
func (s *Multipath) getPathDiscoveryInfo(prefix string) *PathDiscoveryInfo {
	val, loaded := s.pdState.LoadOrStore(prefix, &PathDiscoveryInfo{
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
	if len(packet.Name) < 3 {
		core.Log.Warn(s, "Received Data with short name", "name", packet.Name)
		return
	}

	dataType := packet.Name[2].String()

	switch dataType {
	case "pd":
		s.handlePathDiscoveryData(packet, pitEntry, inFace)
	case "dt":
		s.handleTransmissionData(packet, pitEntry, inFace)
	default:
		core.Log.Warn(s, "Unknown Data type", "type", dataType, "name", packet.Name)
	}
}

func (s *Multipath) AfterReceiveInterest(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	inFace uint64,
	nexthops []*table.FibNextHopEntry,
) {
	// Extract sequence number from interest name and log every 200 packets
	interestName := packet.Name.String()
	seqNum := extractSeqNum(interestName)
	if seqNum >= 0 && seqNum%200 == 0 {
		core.Log.Info(s, "Interest Forwarded",
			"name", interestName,
			"seq", seqNum)
	}

	if len(nexthops) == 0 {
		core.Log.Warn(s, "No nexthops available for", packet.Name)
		return
	}

	// 1. Sort Nexthops to ensure deterministic order for the Round-Robin.
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

	// 3. Retrieve or Initialize the Counter for this Prefix
	val, _ := s.pdState.LoadOrStore(prefixKey, new(uint64))
	counterPtr := val.(*uint64)

	// 4. Atomic Round-Robin Selection
	currentCount := atomic.AddUint64(counterPtr, 1) - 1

	// 5. Calculate Index
	selectedIndex := currentCount % uint64(len(sortedHops))
	targetFace := sortedHops[selectedIndex].Nexthop

	// 6. Send Immediately
	s.SendInterest(packet, pitEntry, targetFace, inFace)
}

func (s *Multipath) BeforeSatisfyInterest(pitEntry table.PitEntry, inFace uint64) {}

// TODO: added by yitong, nack pipeline
func (s *Multipath) AfterReceiveNack(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64, nackReason uint64) {
	// Default behavior: Forward NACK to all downstream faces (except the one it came from)
	// This is the standard NFD behavior - when a NACK is received, forward it downstream
	// so consumers know the Interest failed

	// core.Log.Trace(s, "AfterReceiveNack", "name", packet.Name, "reason", nackReason, "inFace", inFace)
	// TODO: switch back to trace later
	core.Log.Info(s, "Nack received", "name", packet.Name, "reason", nackReason, "inFace", inFace)

	// Forward NACK to all downstream faces (in-records) except the incoming face
	for faceID := range pitEntry.InRecords() {
		if faceID != inFace {
			s.SendNack(packet, pitEntry, faceID, inFace, nackReason)
		}
	}
}

func (s *Multipath) handlePathDiscoveryData(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
	// 1. Lookup FIB to get Prefix
	prefix := "/"
	if len(packet.Name) > 0 {
		prefix = packet.Name[0].String()
	}

	info := s.getPathDiscoveryInfo(prefix)

	// --- FIX START: Decode the full packet ---
	// We must decode the raw wire (TLV) to access the internal Content.
	// packet.Raw is enc.Wire. Use spec.Spec{}.ReadData to decode it.
	parsedData, _, err := spec.Spec{}.ReadData(enc.NewWireView(packet.Raw))
	if err != nil {
		core.Log.Error(s, "Failed to decode Data packet", "err", err, "name", packet.Name)
		// Forward anyway for robustness
		for faceID := range pitEntry.InRecords() {
			s.SendData(packet, pitEntry, faceID, inFace)
		}
		return
	}

	// 2. Deserialize Metadata using ONLY the payload
	// parsedData.Content() returns enc.Wire. We Join() it to get the []byte payload.
	dp, err := DeserializeDiscovery(parsedData.Content().Join())
	if err != nil {
		core.Log.Error(s, "Failed to deserialize DiscoveryPacket", "err", err)
		return
	}
	// --- FIX END ---

	info.mu.Lock()
	defer info.mu.Unlock()

	// 3. Process Convergence Logic
	if dp.TierConverged {
		if !info.IsPdFrozen {
			delete(info.ConvergedUpstreamTier, inFace)
		}

		if len(info.ConvergedUpstreamTier) == 0 {
			dp.TierConverged = true
			// Check if local node converged
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
		delete(info.ConvergedUpstreamNode, inFace)

		if info.IsNodeConverged && len(info.ConvergedUpstreamNode) == 0 {
			dp.NodeConverged = true
		} else {
			dp.NodeConverged = false
		}
	} else {
		dp.NodeConverged = false
	}

	// 4. Serialize updated metadata
	newContent, err := dp.SerializeDiscovery()
	if err != nil {
		core.Log.Error(s, "Failed to serialize DiscoveryPacket", "err", err)
		return
	}

	// --- FIX START: Re-construct and Re-encode the Packet ---

	// Type assert to concrete type to modify ContentV directly
	data, ok := parsedData.(*spec.Data)
	if !ok {
		core.Log.Error(s, "Failed to type assert Data packet")
		return
	}

	// Update the content in our parsed Data object.
	// We wrap the new []byte in enc.Wire.
	// Note: This invalidates the original signature. Since this is a signaling packet
	// processed by the strategy, we assume downstream nodes either don't verify
	// signatures for "pd" packets or we accept the invalidation.
	data.ContentV = enc.Wire{newContent}

	// Encode the modified Data object back to wire format (TLV).
	// Use PacketEncoder to encode the full packet with TLV type 0x06 for Data.
	packetEncoder := spec.PacketEncoder{}
	packetEncoder.Init(&spec.Packet{Data: data})
	newWire := packetEncoder.Encode(&spec.Packet{Data: data})

	// Update the existing packet wrapper with the new wire bytes.
	// We modify packet.Raw because packet.Content does not exist in defn.Pkt.
	packet.Raw = newWire

	// --- FIX END ---

	for faceID := range pitEntry.InRecords() {
		s.SendData(packet, pitEntry, faceID, inFace)
	}
}

// TODO: We need to implement the "dt" mode congestion control later
// handleTransmissionData processes "dt" (DT) packets
func (s *Multipath) handleTransmissionData(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
	// 1. Decode the full packet to access the internal Content
	// packet.Raw is enc.Wire. Use spec.Spec{}.ReadData to decode it.
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
	// parsedData.Content() returns enc.Wire. We Join() it to get the []byte payload.
	// DeserializeData expects the payload (1216 bytes), not the full TLV-encoded packet.
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

// localNodeConverged handles the logic when the local node converges
func (s *Multipath) localNodeConverged(info *PathDiscoveryInfo) {
	if !info.IsNodeConverged {
		// Clear converged upstream tier set
		info.ConvergedUpstreamTier = make(map[uint64]bool)
		info.IsNodeConverged = true

		// Update Tier List based on Wait List (Finalizing Valid Upstreams)
		s.updateAfterPathDiscovery(info)

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

// /* Multipath Strategy - MARS project
//  *
//  * Implemented by Yitong, 2025.12.04
//  * Modified for Per-Prefix Round-Robin, 2025.12.04
//  * Description: Round-Robin forwarding with independent counters per prefix.
//  * Robust against dynamic nexthop changes.
//  */

// package fw

// import (
// 	"sort"
// 	"strconv"
// 	"strings"
// 	"sync"
// 	"sync/atomic"

// 	"github.com/named-data/ndnd/fw/core"
// 	"github.com/named-data/ndnd/fw/defn"
// 	"github.com/named-data/ndnd/fw/table"
// )

// type Multipath struct {
// 	StrategyBase
// 	// perPrefixCounters stores a pointer to a uint64 for each prefix string.
// 	// Key: Prefix String (e.g., "/pro0app"), Value: *uint64
// 	// We use sync.Map for thread-safe concurrent access without global locking.
// 	perPrefixCounters sync.Map
// }

// func init() {
// 	strategyInit = append(strategyInit, func() Strategy { return &Multipath{} })
// 	StrategyVersions["multipath"] = []uint64{1}
// }

// func (s *Multipath) Instantiate(fwThread *Thread) {
// 	s.NewStrategyBase(fwThread, "multipath", 1)
// }

// func (s *Multipath) AfterContentStoreHit(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
// 	s.SendData(packet, pitEntry, inFace, 0)
// }

// func (s *Multipath) AfterReceiveData(packet *defn.Pkt, pitEntry table.PitEntry, inFace uint64) {
// 	// Extract sequence number from data name and log every 200 packets
// 	dataName := packet.Name.String()
// 	seqNum := extractSeqNum(dataName)
// 	if seqNum >= 0 && seqNum%200 == 0 {
// 		core.Log.Info(s, "Data Forwarded",
// 			"name", dataName,
// 			"seq", seqNum)
// 	}

// 	for faceID := range pitEntry.InRecords() {
// 		s.SendData(packet, pitEntry, faceID, inFace)
// 	}
// }

// func (s *Multipath) AfterReceiveInterest(
// 	packet *defn.Pkt,
// 	pitEntry table.PitEntry,
// 	inFace uint64,
// 	nexthops []*table.FibNextHopEntry,
// ) {
// 	// Extract sequence number from interest name and log every 200 packets
// 	interestName := packet.Name.String()
// 	seqNum := extractSeqNum(interestName)
// 	if seqNum >= 0 && seqNum%200 == 0 {
// 		core.Log.Info(s, "Interest Forwarded",
// 			"name", interestName,
// 			"seq", seqNum)
// 	}

// 	if len(nexthops) == 0 {
// 		core.Log.Warn(s, "No nexthops available for", packet.Name)
// 		return
// 	}

// 	// 1. Sort Nexthops to ensure deterministic order for the Round-Robin.
// 	// This ensures that index 0 always refers to the same face ID (as long as that face exists).
// 	sortedHops := make([]*table.FibNextHopEntry, len(nexthops))
// 	copy(sortedHops, nexthops)

// 	sort.Slice(sortedHops, func(i, j int) bool {
// 		return sortedHops[i].Nexthop < sortedHops[j].Nexthop
// 	})

// 	// 2. Identify the Flow/Prefix
// 	// We use the first component (e.g., "/pro0app") as the key for the Round-Robin counter.
// 	// If we used the full name, every chunk would get a new counter (0), defeating RR.
// 	var prefixKey string
// 	if len(packet.Name) > 0 {
// 		prefixKey = packet.Name[0].String()
// 	} else {
// 		prefixKey = "/"
// 	}

// 	// 3. Retrieve or Initialize the Counter for this Prefix
// 	// LoadOrStore returns the existing value if present, or stores the new one.
// 	// We store a pointer to uint64 so we can atomically increment the value *inside* the interface{}.
// 	val, _ := s.perPrefixCounters.LoadOrStore(prefixKey, new(uint64))
// 	counterPtr := val.(*uint64)

// 	// 4. Atomic Round-Robin Selection
// 	// Increment the counter for this specific prefix.
// 	currentCount := atomic.AddUint64(counterPtr, 1) - 1

// 	// 5. Calculate Index (Robust against Link Failure)
// 	// We calculate the index based on the *current* number of nexthops.
// 	// If a link failed and len(sortedHops) decreased, the Modulo operator (%)
// 	// automatically wraps the high counter value into the new, smaller range.
// 	// e.g., Counter=5, Hops=5 -> Index=0.
// 	//       Link fails, Hops=4. Next packet Counter=6 -> Index=2. Valid.
// 	selectedIndex := currentCount % uint64(len(sortedHops))
// 	targetFace := sortedHops[selectedIndex].Nexthop

// 	// 6. Log the decision
// 	// core.Log.Info(s, "Per-Prefix RR Forwarding",
// 	// 	"Prefix", prefixKey,
// 	// 	"Total_Hops", len(sortedHops),
// 	// 	"Selected_Index", selectedIndex,
// 	// 	"Target_Face", targetFace)

// 	// 7. Send Immediately
// 	s.SendInterest(packet, pitEntry, targetFace, inFace)
// }

// func (s *Multipath) BeforeSatisfyInterest(pitEntry table.PitEntry, inFace uint64) {}

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
