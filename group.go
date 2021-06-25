package clear

import (
	"errors"
	"hash/crc32"
	"sync"
)

const (
	StopSign         = "_stop_group"
	workerBufferSize = 2
)

var (
	GroupHasStoppedErr = errors.New("group has stopped err")
)

type TaskFun func(interface{})

func NewGroup(size uint8, f TaskFun) *Group {
	g := new(Group)
	g.initSize(size)
	g.f = f
	g.input = make(chan *Item, workerBufferSize*size)
	g.workerChanMap = make(map[uint8]chan *Item)
	g.initWorker()
	return g
}

type Item struct {
	key   string
	crc32 uint32
	data  interface{}
}

type Group struct {
	f             TaskFun
	input         chan *Item
	size          uint8 // 槽位
	workerChanMap map[uint8]chan *Item
	wg            sync.WaitGroup
	stopped       bool
}

func (g *Group) initSize(size uint8) {
	if size < 10 {
		size = 10
	} else {
		size = size / 10 * 10
	}
	g.size = size
}

// initDistributeWorker 分发任务
func (g *Group) initDistributeWorker() {
	for task := range g.input {
		index := task.crc32 % uint32(g.size)
		if c, ok := g.workerChanMap[uint8(index)]; ok {
			c <- task
		}
	}
	for _, c := range g.workerChanMap {
		// 关闭worker通道
		close(c)
	}
}

func (g *Group) initWorker() {
	for i := uint8(0); i < g.size; i++ {
		g.newWorker(i)
	}
	go g.initDistributeWorker()
}

func (g *Group) newWorker(index uint8) {
	g.wg.Add(1)
	c := make(chan *Item)
	g.workerChanMap[index] = c
	go func() {
		defer g.wg.Done()
		for item := range c {
			g.f(item.data)
		}
	}()
}

func (g *Group) Add(key string, data interface{}) {
	if g.stopped {
		return
	}
	if key == StopSign {
		g.stopped = true
		close(g.input)
		g.wg.Wait() // 退出时是阻塞的
		return
	}
	crc := crc32.ChecksumIEEE([]byte(key))
	g.input <- &Item{
		key:   key,
		crc32: crc,
		data:  data,
	}
}

func (g *Group) Sync() {
	g.Add(StopSign, nil)
}
