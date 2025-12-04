/* YaNFD - Yet another NDN Forwarding Daemon
 *
 * Copyright (C) 2020-2021 Eric Newberry.
 *
 * This file is licensed under the terms of the MIT License, as found in LICENSE.md.
 */

package fw

import (
	"github.com/named-data/ndnd/fw/core"
	enc "github.com/named-data/ndnd/std/encoding"
)

// FwQueueSize is the maxmimum number of packets that can be buffered to be processed by a forwarding thread.
func CfgFwQueueSize() int {
	return core.C.Fw.QueueSize
}

// NumFwThreads indicates the number of forwarding threads in the forwarder.
func CfgNumThreads() int {
	return core.C.Fw.Threads
}

// LockThreadsToCores indicates whether forwarding threads will be locked to cores.
func CfgLockThreadsToCores() bool {
	return core.C.Fw.LockThreadsToCores
}

// TODO: added by Yitong
// CfgNodeName returns the configured Node Name as an NDN Name object.
func CfgNodeName() enc.Name {
	// Parse the string from the config into an NDN Name
	name, err := enc.NameFromStr(core.C.Fw.StrategyNodeName)
	if err != nil {
		// Fallback if configuration is invalid
		core.Log.Error(nil, "Invalid Node Name in config, using default", "err", err)
		name, _ = enc.NameFromStr("/my-node")
	}
	return name
}
