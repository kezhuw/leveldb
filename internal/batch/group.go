package batch

type Request struct {
	Sync  bool
	Batch Batch
	Reply chan error
}

type Group struct {
	Head         Request
	pending      Request
	replys       []chan error
	batchSize    int
	maxBatchSize int
}

func (g *Group) Empty() bool {
	return g.Head.Reply == nil
}

func (g *Group) HasPending() bool {
	return g.pending.Reply != nil
}

func (g *Group) setFirst(req Request) {
	g.Head = req
	g.Head.Batch.Pin()
	g.batchSize = g.Head.Batch.Size()
	switch {
	case g.batchSize <= (128 << 10):
		g.maxBatchSize = g.batchSize + (128 << 10)
	default:
		g.maxBatchSize = 1 << 20
	}
}

func (g *Group) Push(req Request) {
	switch {
	case g.HasPending():
		panic("leveldb: pending in batch group")
	case g.Empty():
		g.setFirst(req)
	case !g.Head.Sync && req.Sync:
		fallthrough
	case g.batchSize > g.maxBatchSize:
		g.pending = req
	case len(g.replys) == 0:
		g.replys = make([]chan error, 1, 2)
		g.replys[0] = g.Head.Reply
		g.Head.Reply = make(chan error, 1)
		fallthrough
	default:
		g.replys = append(g.replys, req.Reply)
		g.Head.Batch.Append(req.Batch.Bytes())
	}
}

func relay(from chan error, replys []chan error) {
	err := <-from
	for _, c := range replys {
		c <- err
	}
}

func (g *Group) Rewind() {
	if g.replys != nil {
		go relay(g.Head.Reply, g.replys)
		g.replys = nil
	}
	switch {
	case g.pending.Reply != nil:
		g.setFirst(g.pending)
		g.pending.Batch.Reset(nil)
		g.pending.Reply = nil
	default:
		g.Head.Batch.Reset(nil)
		g.Head.Reply = nil
	}
}

func (g *Group) Close(err error) {
	switch {
	case g.Empty():
		return
	case g.replys == nil:
		g.Head.Reply <- err
	default:
		for _, c := range g.replys {
			c <- err
		}
		g.replys = nil
	}
	g.Head.Batch.Reset(nil)
	g.Head.Reply = nil
	if g.pending.Reply != nil {
		g.pending.Reply <- err
		g.pending.Batch.Reset(nil)
		g.pending.Reply = nil
	}
}
