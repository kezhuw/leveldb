package request

const (
	firstRequest = 0
	lastRequest  = 2
	totalRequest = 3
)

type Group struct {
	requests [totalRequest]Request
	replys   [totalRequest][]chan error
	current  int

	batchSize    int
	maxBatchSize int
}

func (g *Group) Head() Request {
	return g.requests[firstRequest]
}

func (g *Group) Empty() bool {
	return g.requests[firstRequest].Reply == nil
}

func (g *Group) Full() bool {
	return g.requests[lastRequest].Reply != nil
}

func (g *Group) setCurrent(req Request) {
	g.requests[g.current] = req
	g.requests[g.current].Batch.Pin()
	g.batchSize = req.Batch.Size()
	g.maxBatchSize = 1 << 20
	// Don't blow small batch too much, this way we limit the time cost
	// for extra writes in memtable and file logging, and hopefully don't
	// slowdown small write too much.
	if g.batchSize <= (128 << 10) {
		g.maxBatchSize = g.batchSize + (128 << 10)
	}
}

func (g *Group) Push(req Request) {
	current := g.current
	switch {
	case g.Full():
		panic("leveldb: push to full group")
	case g.batchSize > g.maxBatchSize:
		g.current++
		g.setCurrent(req)
	case g.requests[current].Reply == nil:
		g.setCurrent(req)
	case !g.requests[current].Sync && req.Sync:
		g.current++
		g.setCurrent(req)
	case len(g.replys[current]) == 0:
		replys := make([]chan error, 1, 16)
		replys[0] = g.requests[current].Reply
		g.replys[current] = replys
		g.requests[current].Reply = make(chan error, 1)
		fallthrough
	default:
		g.batchSize += req.Batch.Size()
		g.requests[current].Batch.Append(req.Batch.Bytes())
		g.replys[current] = append(g.replys[current], req.Reply)
	}
}

func relay(from chan error, replys []chan error) {
	err := <-from
	for _, c := range replys {
		c <- err
	}
}

func (g *Group) Rewind() {
	if replys := g.replys[0]; replys != nil {
		go relay(g.requests[0].Reply, replys)
	}
	current := g.current
	switch current {
	case 0:
		g.batchSize = 0
		g.maxBatchSize = 0
	default:
		i := 0
		for i < current {
			j := i + 1
			g.replys[i] = g.replys[j]
			g.requests[i] = g.requests[j]
			i = j
		}
		g.current = current - 1
	}
	g.replys[current] = nil
	g.requests[current] = Request{}
}

func (g *Group) Close(err error) {
	for i, current := 0, g.current; i <= current; i++ {
		switch {
		case g.replys[i] != nil:
			for _, reply := range g.replys[i] {
				reply <- err
			}
			g.replys[i] = nil
		case g.requests[i].Reply != nil:
			g.requests[i].Reply <- err
		}
		g.requests[i] = Request{}
	}
	g.batchSize = 0
	g.maxBatchSize = 0
}
