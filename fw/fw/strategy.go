/* YaNFD - Yet another NDN Forwarding Daemon
 *
 * Copyright (C) 2020-2021 Eric Newberry.
 *
 * This file is licensed under the terms of the MIT License, as found in LICENSE.md.
 */

package fw

import (
	"fmt"

	"github.com/named-data/ndnd/fw/defn"
	"github.com/named-data/ndnd/fw/table"
	enc "github.com/named-data/ndnd/std/encoding"
)

// Strategy represents a forwarding strategy.
type Strategy interface {
	Instantiate(fwThread *Thread)
	String() string
	GetName() enc.Name

	AfterContentStoreHit(
		packet *defn.Pkt,
		pitEntry table.PitEntry,
		inFace uint64)
	AfterReceiveData(
		packet *defn.Pkt,
		pitEntry table.PitEntry,
		inFace uint64)
	AfterReceiveInterest(
		packet *defn.Pkt,
		pitEntry table.PitEntry,
		inFace uint64,
		nexthops []*table.FibNextHopEntry)
	// TODO: added by yitong, nack pipeline
	AfterReceiveNack(
		packet *defn.Pkt,
		pitEntry table.PitEntry,
		inFace uint64,
		nackReason uint64)
	BeforeSatisfyInterest(
		pitEntry table.PitEntry,
		inFace uint64)
}

// StrategyBase provides common helper methods for YaNFD forwarding strategies.
type StrategyBase struct {
	thread   *Thread
	threadID int
	name     enc.Name
	version  uint64
	logName  string

	//TODO: added by Yitong
	StrategyNodeName enc.Name
}

// NewStrategyBase is a helper that allows specific strategies to initialize the base.
func (s *StrategyBase) NewStrategyBase(
	fwThread *Thread,
	name string,
	version uint64,
) {
	s.thread = fwThread
	s.threadID = s.thread.threadID
	s.name = defn.STRATEGY_PREFIX.
		Append(enc.NewGenericComponent(name)).
		Append(enc.NewVersionComponent(version))
	s.version = version
	s.logName = name

	//TODO: added by Yitong
	s.StrategyNodeName = CfgNodeName()
}

// (AI GENERATED DESCRIPTION): Returns a formatted string containing the strategy's log name, version, and thread ID.
func (s *StrategyBase) String() string {
	return fmt.Sprintf("%s (v=%d t=%d)", s.logName, s.version, s.threadID)
}

// GetName returns the name of strategy, including version information.
func (s *StrategyBase) GetName() enc.Name {
	return s.name
}

// SendInterest sends an Interest on the specified face.
func (s *StrategyBase) SendInterest(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	nexthop uint64,
	inFace uint64,
) bool {
	return s.thread.processOutgoingInterest(packet, pitEntry, nexthop, inFace)
}

// SendData sends a Data packet on the specified face.
func (s *StrategyBase) SendData(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	nexthop uint64,
	inFace uint64,
) {
	var pitToken []byte
	if inRecord, ok := pitEntry.InRecords()[nexthop]; ok {
		pitToken = inRecord.PitToken
		pitEntry.RemoveInRecord(nexthop)
	}
	s.thread.processOutgoingData(packet, nexthop, pitToken, inFace)
}

// TODO: added by yitong, nack pipeline
// SendNack sends a NACK packet on the specified face.
func (s *StrategyBase) SendNack(
	packet *defn.Pkt,
	pitEntry table.PitEntry,
	nexthop uint64,
	inFace uint64,
	reason uint64,
) bool {
	//! NackReason is set when this packet is a NACK (Network NACK).
	// It contains the NACK reason code (0 = None, 50 = Congestion, 100 = Duplicate, 150 = NoRoute).
	return s.thread.processOutgoingNack(packet, pitEntry, nexthop, inFace, reason)
}
