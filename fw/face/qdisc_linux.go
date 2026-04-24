//go:build linux

package face

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/named-data/ndnd/fw/core"
)

const (
	qdiscBacklogPollInterval = time.Millisecond

	tcmsgLen = 20

	tcaStats  = 3
	tcaStats2 = 7

	tcaStatsQueue = 3
)

var (
	qdiscBacklogSamplesByIfIndex sync.Map
	qdiscBacklogSamplerOnce      sync.Once
)

type qdiscBacklogSample struct {
	backlog *transportLinkBacklog
	ifName  string
	ifIndex int
}

type qdiscBacklogCounters struct {
	bytes   uint64
	packets uint64
}

func maybeStartQdiscBacklogSampler(localIP net.IP, tag any) *transportLinkBacklog {
	if localIP == nil || localIP.IsUnspecified() {
		return nil
	}

	iface, err := interfaceByLocalIP(localIP)
	if err != nil {
		core.Log.Warn(nil, "Unable to resolve qdisc interface for UDP face",
			"localIP", localIP.String(),
			"transport", tag,
			"err", err)
		return nil
	}

	sample := &qdiscBacklogSample{
		backlog: &transportLinkBacklog{},
		ifName:  iface.Name,
		ifIndex: iface.Index,
	}
	actual, loaded := qdiscBacklogSamplesByIfIndex.LoadOrStore(iface.Index, sample)
	state := actual.(*qdiscBacklogSample)
	if !loaded {
		core.Log.Info(nil, "Registered qdisc backlog interface",
			"transport", tag,
			"interface", state.ifName,
			"ifindex", state.ifIndex)
	}
	qdiscBacklogSamplerOnce.Do(func() {
		core.Log.Info(nil, "Started global qdisc backlog sampler",
			"period", qdiscBacklogPollInterval)
		go runQdiscBacklogSampler()
	})
	return state.backlog
}

func interfaceByLocalIP(ip net.IP) (*net.Interface, error) {
	target4 := ip.To4()
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}
	for _, iface := range ifaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var addrIP net.IP
			switch typed := addr.(type) {
			case *net.IPNet:
				addrIP = typed.IP
			case *net.IPAddr:
				addrIP = typed.IP
			default:
				continue
			}
			if target4 != nil {
				if addrIP.To4() != nil && addrIP.To4().Equal(target4) {
					return &iface, nil
				}
			} else if addrIP.Equal(ip) {
				return &iface, nil
			}
		}
	}
	return nil, errors.New("no interface owns local IP")
}

func runQdiscBacklogSampler() {
	ticker := time.NewTicker(qdiscBacklogPollInterval)
	defer ticker.Stop()

	var lastWarn time.Time
	for !core.ShouldQuit {
		if err := sampleAllRegisteredQdiscBacklogs(); err != nil && time.Since(lastWarn) >= time.Second {
			lastWarn = time.Now()
			core.Log.Warn(nil, "Unable to sample qdisc backlog",
				"err", err)
		}
		<-ticker.C
	}
}

func sampleAllRegisteredQdiscBacklogs() error {
	messages, err := dumpQdiscNetlink()
	if err != nil {
		return err
	}

	backlogs := make(map[int]qdiscBacklogCounters)
	for _, msg := range messages {
		if msg.Header.Type != syscall.RTM_NEWQDISC {
			continue
		}
		if len(msg.Data) < tcmsgLen {
			continue
		}
		ifIndex := int(native.Uint32(msg.Data[4:8]))
		bytes, packets := parseQdiscBacklogFromAttrs(msg.Data[align4(tcmsgLen):])
		if bytes == 0 && packets == 0 {
			continue
		}
		previous := backlogs[ifIndex]
		previous.bytes += bytes
		previous.packets += packets
		backlogs[ifIndex] = previous
	}

	qdiscBacklogSamplesByIfIndex.Range(func(_, value any) bool {
		sample := value.(*qdiscBacklogSample)
		counters := backlogs[sample.ifIndex]
		sample.backlog.bytes.Store(counters.bytes)
		sample.backlog.packets.Store(counters.packets)
		return true
	})
	return nil
}

func parseQdiscBacklogFromAttrs(attrData []byte) (uint64, uint64) {
	attrs := parseRtAttrs(attrData)
	if stats2, ok := attrs[tcaStats2]; ok {
		nested := parseRtAttrs(stats2)
		if queue, ok := nested[tcaStatsQueue]; ok && len(queue) >= 8 {
			return uint64(native.Uint32(queue[4:8])), uint64(native.Uint32(queue[0:4]))
		}
	}
	if stats, ok := attrs[tcaStats]; ok && len(stats) >= 36 {
		return uint64(native.Uint32(stats[32:36])), uint64(native.Uint32(stats[28:32]))
	}
	return 0, 0
}

func dumpQdiscNetlink() ([]syscall.NetlinkMessage, error) {
	fd, err := syscall.Socket(syscall.AF_NETLINK, syscall.SOCK_RAW|syscall.SOCK_CLOEXEC, syscall.NETLINK_ROUTE)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(fd)

	if err := syscall.Bind(fd, &syscall.SockaddrNetlink{Family: syscall.AF_NETLINK}); err != nil {
		return nil, err
	}

	seq := uint32(time.Now().UnixNano())
	req := make([]byte, syscall.NLMSG_HDRLEN+tcmsgLen)
	native.PutUint32(req[0:4], uint32(len(req)))
	native.PutUint16(req[4:6], syscall.RTM_GETQDISC)
	native.PutUint16(req[6:8], syscall.NLM_F_REQUEST|syscall.NLM_F_DUMP)
	native.PutUint32(req[8:12], seq)
	req[syscall.NLMSG_HDRLEN] = syscall.AF_UNSPEC

	if err := syscall.Sendto(fd, req, 0, &syscall.SockaddrNetlink{Family: syscall.AF_NETLINK}); err != nil {
		return nil, err
	}

	var all []syscall.NetlinkMessage
	buf := make([]byte, 1<<20)
	for {
		n, _, err := syscall.Recvfrom(fd, buf, 0)
		if err != nil {
			return nil, err
		}
		msgs, err := syscall.ParseNetlinkMessage(buf[:n])
		if err != nil {
			return nil, err
		}
		for _, msg := range msgs {
			if msg.Header.Seq != seq {
				continue
			}
			switch msg.Header.Type {
			case syscall.NLMSG_DONE:
				return all, nil
			case syscall.NLMSG_ERROR:
				if len(msg.Data) >= 4 {
					code := int32(native.Uint32(msg.Data[:4]))
					if code == 0 {
						return all, nil
					}
					return nil, syscall.Errno(-code)
				}
				return nil, errors.New("netlink qdisc error")
			default:
				all = append(all, msg)
			}
		}
	}
}

func parseRtAttrs(b []byte) map[uint16][]byte {
	attrs := make(map[uint16][]byte)
	for len(b) >= 4 {
		attrLen := int(native.Uint16(b[0:2]))
		attrType := native.Uint16(b[2:4])
		if attrLen < 4 || attrLen > len(b) {
			break
		}
		attrs[attrType] = b[4:attrLen]
		b = b[align4(attrLen):]
	}
	return attrs
}

func align4(n int) int {
	return (n + 3) &^ 3
}

var native binary.ByteOrder = binary.LittleEndian
