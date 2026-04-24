//go:build !linux

package face

import "net"

func maybeStartQdiscBacklogSampler(net.IP, any) *transportLinkBacklog {
	return nil
}
