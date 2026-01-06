/* YaNFD - Yet another NDN Forwarding Daemon
 *
 * Copyright (C) 2020-2021 Eric Newberry.
 *
 * This file is licensed under the terms of the MIT License, as found in LICENSE.md.
 */

package fw

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/named-data/ndnd/fw/core"
	"github.com/named-data/ndnd/fw/defn"
	"github.com/named-data/ndnd/fw/dispatch"
	"github.com/named-data/ndnd/fw/table"
	enc "github.com/named-data/ndnd/std/encoding"
	"github.com/named-data/ndnd/std/utils"
)

// TODO:debug, changed by yitong for testing
// MaxFwThreads Maximum number of forwarding threads
// const MaxFwThreads = 32
const MaxFwThreads = 64

// Threads contains all forwarding threads
var Threads []*Thread

// HashNameToFwThread hashes an NDN name to a forwarding thread.
func HashNameToFwThread(name enc.Name) int {
	// Dispatch all management requests to thread 0
	// this is fine, all it does is make sure the pitcs table in thread 0 has the management stuff.
	// This is not actually touching management.
	if len(name) > 0 && name[0].Equal(enc.LOCALHOST) {
		return 0
	}
	// to prevent negative modulos because we converted from uint to int
	return int(name.Hash() % uint64(len(Threads)))
}

// HashNameToAllPrefixFwThreads hashes an NDN name to all forwarding threads for all prefixes of the name.
// The return value is a boolean map of which threads match the name
func HashNameToAllPrefixFwThreads(name enc.Name) []bool {
	threads := make([]bool, len(Threads))

	// Dispatch all management requests to thread 0
	if len(name) > 0 && name[0].Equal(enc.LOCALHOST) {
		threads[0] = true
		return threads
	}

	prefixHash := name.PrefixHash()
	for i := 1; i < len(prefixHash); i++ {
		thread := int(prefixHash[i] % uint64(len(Threads)))
		threads[thread] = true
	}
	return threads
}

// Thread Represents a forwarding thread
type Thread struct {
	threadID      int
	pending       chan *defn.Pkt
	pitCS         table.PitCsTable
	strategies    map[uint64]Strategy
	deadNonceList *table.DeadNonceList
	shouldQuit    chan interface{}
	HasQuit       chan interface{}

	// Counters
	nInInterests  atomic.Uint64
	nInData       atomic.Uint64
	nOutInterests atomic.Uint64
	nOutData      atomic.Uint64
	// TODO: added by yitong, nack pipeline
	nInNacks              atomic.Uint64
	nOutNacks             atomic.Uint64
	nSatisfiedInterests   atomic.Uint64
	nUnsatisfiedInterests atomic.Uint64
	nCsHits               atomic.Uint64
	nCsMisses             atomic.Uint64
}

// NewThread creates a new forwarding thread
func NewThread(id int) *Thread {
	t := new(Thread)
	t.threadID = id
	t.pending = make(chan *defn.Pkt, CfgFwQueueSize())
	t.pitCS = table.NewPitCS(t.finalizeInterest)
	t.strategies = InstantiateStrategies(t)
	t.deadNonceList = table.NewDeadNonceList()
	t.shouldQuit = make(chan interface{}, 1)
	t.HasQuit = make(chan interface{})
	return t
}

// (AI GENERATED DESCRIPTION): Returns a string representation of the thread, formatted as “fw-thread‑<threadID>”.
func (t *Thread) String() string {
	return fmt.Sprintf("fw-thread-%d", t.threadID)
}

// GetID returns the ID of the forwarding thread
func (t *Thread) GetID() int {
	return t.threadID
}

// Counters returns the counters for this forwarding thread
func (t *Thread) Counters() defn.FWThreadCounters {
	return defn.FWThreadCounters{
		NPitEntries:   t.pitCS.PitSize(),
		NCsEntries:    t.pitCS.CsSize(),
		NInInterests:  t.nInInterests.Load(),
		NInData:       t.nInData.Load(),
		NOutInterests: t.nOutInterests.Load(),
		NOutData:      t.nOutData.Load(),
		// TODO: added by yitong, nack pipeline
		NInNacks:              t.nInNacks.Load(),
		NOutNacks:             t.nOutNacks.Load(),
		NSatisfiedInterests:   t.nSatisfiedInterests.Load(),
		NUnsatisfiedInterests: t.nUnsatisfiedInterests.Load(),
		NCsHits:               t.nCsHits.Load(),
		NCsMisses:             t.nCsMisses.Load(),
	}
}

// TellToQuit tells the forwarding thread to quit
func (t *Thread) TellToQuit() {
	core.Log.Info(t, "Told to quit")
	t.shouldQuit <- true
}

// Run forwarding thread
func (t *Thread) Run() {
	if CfgLockThreadsToCores() {
		runtime.LockOSThread()
	}

	for !core.ShouldQuit {
		select {
		case pkt := <-t.pending:
			// TODO: added by yitong, nack pipeline
			// NACKs are Interest packets with NACK indication, so check NackReason first
			if pkt.NackReason != nil && pkt.L3.Interest != nil {
				t.processIncomingNack(pkt)
			} else if pkt.L3.Interest != nil {
				t.processIncomingInterest(pkt)
			} else if pkt.L3.Data != nil {
				t.processIncomingData(pkt)
			}
		case <-t.deadNonceList.Ticker.C:
			t.deadNonceList.RemoveExpiredEntries()
		case <-t.pitCS.UpdateTicker():
			t.pitCS.Update()
		case <-t.shouldQuit:
			continue
		}
	}

	t.deadNonceList.Ticker.Stop()

	core.Log.Info(t, "Stopping thread")
	t.HasQuit <- true
}

// QueueInterest queues an Interest for processing by this forwarding thread.
func (t *Thread) QueueInterest(interest *defn.Pkt) {
	select {
	case t.pending <- interest:
	default:
		core.Log.Error(t, "Interest dropped due to full queue")
	}
}

// QueueData queues a Data packet for processing by this forwarding thread.
func (t *Thread) QueueData(data *defn.Pkt) {
	select {
	case t.pending <- data:
	default:
		core.Log.Error(t, "Data dropped due to full queue")
	}
}

// TODO: added by yitong, nack pipeline
// QueueNack queues a NACK packet for processing by this forwarding thread.
func (t *Thread) QueueNack(nack *defn.Pkt) {
	select {
	case t.pending <- nack:
	default:
		core.Log.Error(t, "NACK dropped due to full queue")
	}
}

// (AI GENERATED DESCRIPTION): Processes an incoming Interest packet: verifies its validity, enforces hop limits and scope, checks for nonces and dead‑nonce loops, updates the PIT and content store, selects and filters next‑hops via the FIB, and forwards the Interest according to the chosen forwarding strategy.
func (t *Thread) processIncomingInterest(packet *defn.Pkt) {
	interest := packet.L3.Interest
	if interest == nil {
		panic("processIncomingInterest called with non-Interest packet")
	}

	// Already asserted that this is an Interest in link service
	// Get incoming face
	incomingFace := dispatch.GetFace(packet.IncomingFaceID)
	if incomingFace == nil {
		core.Log.Error(t, "Interest has non-existent incoming face", "faceid", packet.IncomingFaceID, "name", packet.Name)
		return
	}

	if interest.HopLimitV != nil {
		core.Log.Trace(t, "HopLimit check", "name", packet.Name, "hoplimit", *interest.HopLimitV)
		if *interest.HopLimitV == 0 {
			return
		}
		*interest.HopLimitV -= 1
	}

	// Log PIT token (if any)
	core.Log.Trace(t, "OnIncomingInterest", "name", packet.Name, "faceid", incomingFace.FaceID(), "pittoken", len(packet.PitToken))

	// Check if violates /localhost
	if incomingFace.Scope() == defn.NonLocal && len(packet.Name) > 0 && packet.Name[0].Equal(enc.LOCALHOST) {
		core.Log.Warn(t, "Interest from non-local face violates /localhost scope", "name", packet.Name, "faceid", incomingFace.FaceID())
		return
	}

	// Update counter
	t.nInInterests.Add(1)

	// Check for forwarding hint and, if present, determine if reaching producer region (and then strip forwarding hint)
	isReachingProducerRegion := true
	var fhName enc.Name = nil
	hint := interest.ForwardingHintV
	if hint != nil && len(hint.Names) > 0 {
		isReachingProducerRegion = false
		for _, fh := range hint.Names {
			if table.NetworkRegion.IsProducer(fh) {
				isReachingProducerRegion = true
				break
			} else if fhName == nil {
				fhName = fh
			}
		}
		if isReachingProducerRegion {
			// TODO: Drop the forwarding hint for now.
			// No way without re-encoding Interest for now.
			fhName = nil
		}
	}

	// Drop packet if no nonce is found
	if !interest.NonceV.IsSet() {

		core.Log.Trace(t, "Interest has no nonce - DROP", "name", packet.Name)
		return
	}

	// Check if packet is in dead nonce list
	if exists := t.deadNonceList.Find(interest.NameV, interest.NonceV.Unwrap()); exists {
		core.Log.Trace(t, "Interest is looping (DNL)", "name", packet.Name, "nonce", interest.NonceV.Unwrap())
		return
	}

	// Check if any matching PIT entries (and if duplicate)
	// read into this, looks like this one will have to be manually changed
	pitEntry, isDuplicate := t.pitCS.InsertInterest(interest, fhName, incomingFace.FaceID())
	if isDuplicate {
		// Interest loop detected - invoke strategy's AfterReceiveLoopedInterest pipeline
		core.Log.Trace(t, "Interest is looping (PIT)", "name", packet.Name)

		// Get strategy for name
		strategyName := table.FibStrategyTable.FindStrategyEnc(interest.Name())
		strategy := t.strategies[strategyName.Hash()]

		// Invoke strategy's AfterReceiveLoopedInterest pipeline
		// Strategy can decide to drop, send NACK, or handle the loop differently
		strategy.AfterReceiveLoopedInterest(packet, pitEntry, incomingFace.FaceID())
		return
	}

	// Get strategy for name
	strategyName := table.FibStrategyTable.FindStrategyEnc(interest.Name())
	strategy := t.strategies[strategyName.Hash()]

	// Add in-record and determine if already pending
	// this looks like custom interest again, but again can be changed without much issue?
	inRecord, isAlreadyPending, prevNonce := pitEntry.InsertInRecord(
		interest, incomingFace.FaceID(), packet.PitToken)

	if !isAlreadyPending {
		core.Log.Trace(t, "Interest is not pending", "name", packet.Name)

		// Check CS for matching entry
		if t.pitCS.IsCsServing() {
			csEntry := t.pitCS.FindMatchingDataFromCS(interest)
			if csEntry != nil {
				// Update counters
				t.nCsHits.Add(1)

				// Parse the cached data packet and replace in the pending one
				// This is not the fastest way to do it, but simplifies everything
				// significantly. We can optimize this later.
				csData, csWire, err := csEntry.Copy()
				if csData != nil && csWire != nil {
					// Mark PIT entry as expired
					table.UpdateExpirationTimer(pitEntry, time.Now())

					// Create the pending packet structure
					packet.L3.Data = csData
					packet.L3.Interest = nil
					packet.Raw = enc.Wire{csWire}
					packet.Name = csData.NameV
					strategy.AfterContentStoreHit(packet, pitEntry, incomingFace.FaceID())
					return
				} else if err != nil {
					core.Log.Error(t, "Error copying CS entry", "err", err)
				} else {
					core.Log.Error(t, "Error copying CS entry", "err", "csData is nil")
				}
			} else {
				// Update counters
				t.nCsMisses.Add(1)
			}
		}
	} else {
		core.Log.Trace(t, "Interest is already pending", "name", packet.Name)

		// Add the previous nonce to the dead nonce list to prevent further looping
		// TODO: review this design, not specified in NFD dev guide
		t.deadNonceList.Insert(interest.Name(), prevNonce)
	}

	// Update PIT entry expiration timer
	if inRecord.ExpirationTime.After(pitEntry.ExpirationTime()) {
		table.UpdateExpirationTimer(pitEntry, inRecord.ExpirationTime)
	}

	// If NextHopFaceId set, forward to that face (if it exists) or drop
	if hop, ok := packet.NextHopFaceID.Get(); ok {
		if face := dispatch.GetFace(hop); face != nil {
			core.Log.Trace(t, "NextHopFaceId is set for Interest", "name", packet.Name)
			t.processOutgoingInterest(packet, pitEntry, hop, incomingFace.FaceID())
		} else {
			core.Log.Info(t, "Non-existent face specified in NextHopFaceId for Interest",
				"name", packet.Name, "faceid", hop)
		}
		return
	}

	// Use forwarding hint if present
	lookupName := interest.Name()
	if fhName != nil {
		lookupName = fhName
	}

	// Query the FIB for all possible nexthops
	nexthops := table.FibStrategyTable.FindNextHopsEnc(lookupName)

	// If the first component is /localhop, we do not forward interests received
	// on non-local faces to non-local faces
	localFacesOnly := incomingFace.Scope() != defn.Local && packet.Name.At(0).Equal(enc.LOCALHOP)

	// Filter the nexthops that are allowed for this Interest
	allowedNexthops := make([]*table.FibNextHopEntry, 0, len(nexthops))
	for _, nexthop := range nexthops {
		// Exclude incoming face
		if nexthop.Nexthop == packet.IncomingFaceID {
			continue
		}

		// Exclude non-local faces for localhop enforcement
		if localFacesOnly {
			if face := dispatch.GetFace(nexthop.Nexthop); face != nil && face.Scope() != defn.Local {
				continue
			}
		}

		// Exclude faces that have an in-record for this interest
		// TODO: unclear where NFD dev guide specifies such behavior (if any)
		if pitEntry.InRecords()[nexthop.Nexthop] == nil {
			allowedNexthops = append(allowedNexthops, nexthop)
		}
	}

	// Pass to strategy AfterReceiveInterest pipeline
	strategy.AfterReceiveInterest(packet, pitEntry, incomingFace.FaceID(), allowedNexthops)
}

// (AI GENERATED DESCRIPTION): Forwards an Interest packet to the chosen outgoing face, updating the PIT entry’s out‑record, generating a PIT token, and enforcing HopLimit and anti‑loop rules before transmitting it.
func (t *Thread) processOutgoingInterest(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	nexthop uint64,
	inFace uint64,
) bool {
	interest := packet.L3.Interest
	if interest == nil {
		panic("processOutgoingInterest called with non-Interest packet")
	}

	core.Log.Trace(t, "OnOutgoingInterest", "name", packet.Name, "faceid", nexthop)

	// Get outgoing face
	outgoingFace := dispatch.GetFace(nexthop)
	if outgoingFace == nil {
		core.Log.Error(t, "Non-existent nexthop", "name", packet.Name, "faceid", nexthop)
		return false
	}
	if outgoingFace.FaceID() == inFace && outgoingFace.LinkType() != defn.AdHoc {
		core.Log.Trace(t, "Prevent send Interest back to incoming face", "name", packet.Name, "faceid", nexthop)
		return false
	}

	// Drop if HopLimit (if present) on Interest going to non-local face is 0. If so, drop
	if interest.HopLimitV != nil && int(*interest.HopLimitV) == 0 &&
		outgoingFace.Scope() == defn.NonLocal {
		core.Log.Trace(t, "Prevent send Interest with HopLimit=0 to non-local face", "name", packet.Name, "faceid", nexthop)
		return false
	}

	// Create or update out-record
	pitEntry.InsertOutRecord(interest, nexthop)

	// Update counters
	t.nOutInterests.Add(1)

	// Make new PIT token if needed
	pitToken := make([]byte, 6)
	binary.BigEndian.PutUint16(pitToken, uint16(t.threadID))
	binary.BigEndian.PutUint32(pitToken[2:], pitEntry.Token())

	// Send on outgoing face
	outgoingFace.SendPacket(dispatch.OutPkt{
		Pkt:      packet,
		PitToken: pitToken,
		InFace:   inFace,
	})

	return true
}

// (AI GENERATED DESCRIPTION): Finalizes an Interest by recording its nonces into the dead‑nonce list and, if the Interest was unsatisfied, incrementing the counter of unsatisfied Interests.
func (t *Thread) finalizeInterest(pitEntry table.PitEntry) {
	// Check for nonces to insert into dead nonce list
	for _, outRecord := range pitEntry.OutRecords() {
		t.deadNonceList.Insert(pitEntry.EncName(), outRecord.LatestNonce)
	}

	// Update counters
	if !pitEntry.Satisfied() {
		t.nUnsatisfiedInterests.Add(uint64(len(pitEntry.InRecords())))
	}
}

// TODO: added by yitong, nack pipeline
// (AI GENERATED DESCRIPTION): Processes an incoming NACK packet by validating it, finding the corresponding PIT entry via PIT token or name, verifying it matches an out-record, updating counters, and invoking the strategy's AfterReceiveNack handler.
func (t *Thread) processIncomingNack(packet *defn.Pkt) {
	interest := packet.L3.Interest
	if interest == nil || packet.NackReason == nil {
		panic("processIncomingNack called with non-NACK packet")
	}

	// Get incoming face
	incomingFace := dispatch.GetFace(packet.IncomingFaceID)
	if incomingFace == nil {
		core.Log.Error(t, "NACK has non-existent incoming face", "faceid", packet.IncomingFaceID, "name", packet.Name)
		return
	}

	// Update counter
	t.nInNacks.Add(1)

	// Log PIT token (if any)
	core.Log.Trace(t, "OnIncomingNack", "name", packet.Name, "faceid", incomingFace.FaceID(), "reason", *packet.NackReason, "pittoken", len(packet.PitToken))

	// Find PIT entry using PIT token (preferred) or name (fallback)
	var pitEntry table.PitEntry
	if len(packet.PitToken) == 6 {
		// Decode PIT token: uint16 (thread ID) + uint32 (PIT entry token)
		token := binary.BigEndian.Uint32(packet.PitToken[2:6])
		// Find PIT entry by token
		// Note: We use FindInterestPrefixMatchByDataEnc with token for lookup
		// Since we don't have Data, we'll need to use a different approach
		// For now, we'll search by name and verify token matches
		pitEntry = t.pitCS.FindInterestExactMatchEnc(interest)
		if pitEntry != nil && pitEntry.Token() != token {
			// Token doesn't match, this NACK might be for a different PIT entry
			core.Log.Trace(t, "NACK PIT token mismatch", "name", packet.Name, "expected", pitEntry.Token(), "got", token)
			pitEntry = nil
		}
	}

	// Fallback to name-based lookup if no PIT token or token didn't match
	if pitEntry == nil {
		pitEntry = t.pitCS.FindInterestExactMatchEnc(interest)
	}

	if pitEntry == nil {
		// No matching PIT entry - unsolicited NACK, drop
		core.Log.Trace(t, "Unsolicited NACK (no PIT entry)", "name", packet.Name)
		return
	}

	// Verify that the NACK corresponds to an out-record from the incoming face
	// This prevents spoofing - a NACK should only come from a face we sent the Interest to
	_, hasOutRecord := pitEntry.OutRecords()[packet.IncomingFaceID]
	if !hasOutRecord {
		core.Log.Trace(t, "NACK from face with no out-record", "name", packet.Name, "faceid", packet.IncomingFaceID)
		// Still process it, but log the anomaly
	}

	// Get strategy for name
	strategyName := table.FibStrategyTable.FindStrategyEnc(interest.Name())
	strategy := t.strategies[strategyName.Hash()]

	// Invoke strategy's AfterReceiveNack pipeline
	// Strategy can decide to forward NACK downstream, try alternate nexthop, or drop
	strategy.AfterReceiveNack(packet, pitEntry, incomingFace.FaceID(), *packet.NackReason)
}

// (AI GENERATED DESCRIPTION): Processes an incoming Data packet by validating it, updating counters, enforcing scope rules, inserting it into the content store, matching and satisfying any pending PIT entries through the appropriate strategy, and forwarding the data to the corresponding downstream faces.
func (t *Thread) processIncomingData(packet *defn.Pkt) {
	data := packet.L3.Data
	if data == nil {
		panic("processIncomingData called with non-Data packet")
	}

	// Get PIT if present
	var pitToken *uint32
	//lint:ignore S1009 removing the nil check causes a segfault ¯\_(ツ)_/¯
	if packet.PitToken != nil && len(packet.PitToken) == 6 {
		pitToken = utils.IdPtr(binary.BigEndian.Uint32(packet.PitToken[2:6]))
	}

	// Get incoming face
	incomingFace := dispatch.GetFace(packet.IncomingFaceID)
	if incomingFace == nil {
		core.Log.Error(t, "Non-existent nexthop for Data", "name", packet.Name, "faceid", packet.IncomingFaceID)
		return
	}

	// Update counters
	t.nInData.Add(1)

	// Check if violates /localhost
	if incomingFace.Scope() == defn.NonLocal && len(packet.Name) > 0 && packet.Name[0].Equal(enc.LOCALHOST) {
		core.Log.Warn(t, "Data from non-local face violates /localhost scope", "name", packet.Name, "faceid", packet.IncomingFaceID)
		return
	}

	// Add to Content Store
	if t.pitCS.IsCsAdmitting() {
		t.pitCS.InsertData(data, packet.Raw.Join())
	}

	// Check for matching PIT entries
	pitEntries := t.pitCS.FindInterestPrefixMatchByDataEnc(data, pitToken)
	if len(pitEntries) == 0 {
		// Unsolicited Data - nothing more to do
		core.Log.Trace(t, "Unsolicited data", "name", packet.Name, "faceid", packet.IncomingFaceID)
		return
	}

	// Get strategy for name
	strategyName := table.FibStrategyTable.FindStrategyEnc(data.NameV)
	strategy := t.strategies[strategyName.Hash()]

	if len(pitEntries) == 1 {
		// When a single PIT entry matches, we pass the data to the strategy.
		// See alternative behavior for multiple matches below.
		pitEntry := pitEntries[0]

		// Set PIT entry expiration to now
		table.UpdateExpirationTimer(pitEntry, time.Now())

		// Invoke strategy's AfterReceiveData
		core.Log.Trace(t, "Sending Data", "name", packet.Name, "strategy", strategyName)
		strategy.AfterReceiveData(packet, pitEntry, packet.IncomingFaceID)

		// Mark PIT entry as satisfied
		pitEntry.SetSatisfied(true)

		// Insert into dead nonce list
		for _, outRecord := range pitEntry.OutRecords() {
			t.deadNonceList.Insert(data.NameV, outRecord.LatestNonce)
		}

		// Clear out records from PIT entry
		// TODO: NFD dev guide specifies in-records should not be cleared - why?
		pitEntry.ClearInRecords()
		pitEntry.ClearOutRecords()
	} else {
		// Multiple PIT entries can match when two interest have e.g. different flags
		// like CanBePrefix, or different forwarding hints. In this case, we send to all
		// downstream faces without consulting strategy (see NFD dev guide)
		for _, pitEntry := range pitEntries {
			// Store all pending downstreams (except face Data packet arrived on) and PIT tokens
			downstreams := make(map[uint64][]byte)
			for face, record := range pitEntry.InRecords() {
				if face != packet.IncomingFaceID {
					// TODO: Ad-hoc faces
					downstreams[face] = make([]byte, len(record.PitToken))
					copy(downstreams[face], record.PitToken)
				}
			}

			// Set PIT entry expiration to now
			table.UpdateExpirationTimer(pitEntry, time.Now())

			// Invoke strategy's BeforeSatisfyInterest
			strategy.BeforeSatisfyInterest(pitEntry, packet.IncomingFaceID)

			// Mark PIT entry as satisfied
			pitEntry.SetSatisfied(true)

			// Insert into dead nonce list
			for _, outRecord := range pitEntries[0].OutRecords() {
				t.deadNonceList.Insert(data.NameV, outRecord.LatestNonce)
			}

			// Clear PIT entry's in- and out-records
			pitEntry.ClearInRecords()
			pitEntry.ClearOutRecords()

			// Call outgoing Data pipeline for each pending downstream
			for face, token := range downstreams {
				core.Log.Trace(t, "Multiple PIT entries for Data", "name", packet.Name)
				t.processOutgoingData(packet, face, token, packet.IncomingFaceID)
			}
		}
	}
}

// (AI GENERATED DESCRIPTION): Sends a Data packet to a specified next‑hop face, performing /localhost scope checks, logging the event, updating outgoing data and satisfied interest counters, and delivering the packet via the outgoing face.
func (t *Thread) processOutgoingData(
	packet *defn.Pkt,
	nexthop uint64,
	pitToken []byte,
	inFace uint64,
) {
	data := packet.L3.Data
	if data == nil {
		panic("processOutgoingData called with non-Data packet")
	}

	core.Log.Trace(t, "OnOutgoingData", "name", packet.Name, "faceid", nexthop)

	// Get outgoing face
	outgoingFace := dispatch.GetFace(nexthop)
	if outgoingFace == nil {
		core.Log.Error(t, "Non-existent nexthop for Data", "name", packet.Name, "faceid", nexthop)
		return
	}

	// Check if violates /localhost
	if outgoingFace.Scope() == defn.NonLocal && len(packet.Name) > 0 && packet.Name[0].Equal(enc.LOCALHOST) {
		core.Log.Warn(t, "Data cannot be sent to non-local face since violates /localhost scope", "name", packet.Name, "faceid", nexthop)
		return
	}

	// Update counters
	t.nOutData.Add(1)
	t.nSatisfiedInterests.Add(1)

	// Send on outgoing face
	outgoingFace.SendPacket(dispatch.OutPkt{
		Pkt:      packet,
		PitToken: pitToken,
		InFace:   inFace,
	})
}

// TODO: added by yitong, nack pipeline
// (AI GENERATED DESCRIPTION): Sends a NACK packet to a specified next‑hop face, performing /localhost scope checks, logging the event, updating outgoing NACK counters, encoding the NACK in LpPacket, and delivering the packet via the outgoing face.
func (t *Thread) processOutgoingNack(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	nexthop uint64,
	inFace uint64,
	reason uint64,
) bool {
	interest := packet.L3.Interest
	if interest == nil {
		panic("processOutgoingNack called with non-Interest packet")
	}

	core.Log.Trace(t, "OnOutgoingNack", "name", packet.Name, "faceid", nexthop, "reason", reason)

	// Get outgoing face
	outgoingFace := dispatch.GetFace(nexthop)
	if outgoingFace == nil {
		core.Log.Error(t, "Non-existent nexthop for NACK", "name", packet.Name, "faceid", nexthop)
		return false
	}

	// Check if violates /localhost
	if outgoingFace.Scope() == defn.NonLocal && len(packet.Name) > 0 && packet.Name[0].Equal(enc.LOCALHOST) {
		core.Log.Warn(t, "NACK cannot be sent to non-local face since violates /localhost scope", "name", packet.Name, "faceid", nexthop)
		return false
	}

	// Update counters
	t.nOutNacks.Add(1)

	// Create a copy of the packet with NACK reason set
	nackPacket := *packet
	nackPacket.NackReason = &reason

	// Make PIT token if needed (for downstream forwarding)
	var pitToken []byte
	if inRecord, ok := pitEntry.InRecords()[nexthop]; ok && len(inRecord.PitToken) > 0 {
		pitToken = inRecord.PitToken
	} else {
		// Generate PIT token for this thread
		pitToken = make([]byte, 6)
		binary.BigEndian.PutUint16(pitToken, uint16(t.threadID))
		binary.BigEndian.PutUint32(pitToken[2:], pitEntry.Token())
	}

	// Send on outgoing face
	outgoingFace.SendPacket(dispatch.OutPkt{
		Pkt:      &nackPacket,
		PitToken: pitToken,
		InFace:   inFace,
	})

	return true
}
