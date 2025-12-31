package defn

type FWThreadCounters struct {
	NPitEntries   int
	NCsEntries    int
	NInInterests  uint64
	NInData       uint64
	NOutInterests uint64
	NOutData      uint64
	// TODO: added by yitong, nack pipeline
	NInNacks              uint64
	NOutNacks             uint64
	NSatisfiedInterests   uint64
	NUnsatisfiedInterests uint64
	NCsHits               uint64
	NCsMisses             uint64
}
