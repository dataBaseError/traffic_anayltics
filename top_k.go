package main

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync/atomic"
	// "strings"
	"sync"
	"time"
)

type Msg struct {
	Site   string
	Id     uint64
	TimeMs int64 // ms unix timestamp
}

type Details struct {
	Site  string
	Count uint64
}

type DataStreamFanout struct {
	in                chan Msg
	partitionMap      map[int]chan Msg
	partitionCount    int
	appCount          int
	appIndex          int
	topListCheckCount int
	receiveWorkers    int

	updateChan chan Details

	topKList              []Details
	topKMapLatest         map[string]uint64
	listMutex             *sync.RWMutex
	receiveCloseWaitGroup *sync.WaitGroup
	closeWaitGroup        *sync.WaitGroup
	closep2WaitGroup      *sync.WaitGroup
}

func NewDataStreamFanout() *DataStreamFanout {
	return &DataStreamFanout{
		in:                    make(chan Msg),
		updateChan:            make(chan Details),
		topKList:              make([]Details, 15),
		listMutex:             &sync.RWMutex{},
		topListCheckCount:     100,
		receiveWorkers:        100,
		receiveCloseWaitGroup: &sync.WaitGroup{},
		topKMapLatest:         make(map[string]uint64),
	}
}

func (d *DataStreamFanout) PrepareData() (msgs []Msg) {
	// rand.Seed(time.Now().UnixNano())
	rand.Seed(1)

	siteList := generateRandomSites(100) // Generate 100 unique random site names
	msgs = generateMsgs(
		1000000*10,
		siteList)
	return msgs
}

func (d *DataStreamFanout) Input(msgs []Msg) {
	for _, m := range msgs {
		d.in <- m
	}
}

func (d *DataStreamFanout) Close() {
	close(d.in)

	d.receiveCloseWaitGroup.Wait()
	for _, v := range d.partitionMap {
		close(v)
	}

	d.closeWaitGroup.Wait()
	close(d.updateChan)

	d.closep2WaitGroup.Wait()

}

func (d *DataStreamFanout) Setup() {
	d.partitionMap = make(map[int]chan Msg)
	d.partitionCount = 50

	d.closeWaitGroup = &sync.WaitGroup{}
	d.closeWaitGroup.Add(d.partitionCount)
	d.closep2WaitGroup = &sync.WaitGroup{}
	d.closep2WaitGroup.Add(d.topListCheckCount)
	d.receiveCloseWaitGroup.Add(d.receiveWorkers)

	for i := 0; i < d.receiveWorkers; i += 1 {
		go d.Receive()
	}

	var char int
	for char = 0; char < d.partitionCount; char += 1 {
		ch := make(chan Msg)
		d.partitionMap[char] = ch

		go d.HandleSubGroupCount(ch)
	}

	for i := 0; i < d.topListCheckCount; i += 1 {
		go d.TopKCheck()
	}

}

func hashURL(url string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(url))
	return int(h.Sum32() % uint32(n))
}

// Parition the input into sub groups based on the site name
func (d *DataStreamFanout) Receive() {
	defer d.receiveCloseWaitGroup.Done()

	for {
		msg, ok := <-d.in
		if !ok {
			return
		}

		ch, ok := d.partitionMap[hashURL(msg.Site, d.partitionCount)]
		if !ok {
			// Mapping failed
			panic("map failed")
		}

		ch <- msg
	}
}

func (d *DataStreamFanout) HandleSubGroupCount(ch chan Msg) {
	countMap := make(map[string]uint64)
	defer d.closeWaitGroup.Done()

	for {
		msg, ok := <-ch
		if !ok {
			return
		}
		// TODO unique test by paritioning

		count := countMap[msg.Site] + 1
		countMap[msg.Site] = count

		d.updateChan <- Details{
			Site:  msg.Site,
			Count: count,
		}
	}
}

// Though it would be nice to be more limited with the lock, we can't have the indexing changing after we do our work.
func (d *DataStreamFanout) UpdateTopList(update Details) {
	d.listMutex.Lock()

	if update.Count <= d.topKList[14].Count {
		d.listMutex.Unlock()
		return
	}

	last, ok := d.topKMapLatest[update.Site]
	if ok && last > update.Count {
		d.listMutex.Unlock()
		return
	}

	index := idxDescending(d.topKList, update)

	prevIndex := -1
	if d.topKList[14].Count <= update.Count-1 {
		for i := index; i < len(d.topKList); i += 1 {
			if d.topKList[i].Site == update.Site {
				prevIndex = i
				break

			}
		}
	}

	start := len(d.topKList) - 1

	if prevIndex > -1 {
		start = prevIndex
	}

	copy(
		d.topKList[index+1:start+1],
		d.topKList[index:start],
	)

	d.topKList[index] = update
	d.topKMapLatest[update.Site] = update.Count
	d.listMutex.Unlock()
}

func (d *DataStreamFanout) TopKCheck() {
	defer d.closep2WaitGroup.Done()
	for {
		update, ok := <-d.updateChan
		if !ok {

			return
		}

		if atomic.LoadUint64(&d.topKList[14].Count) >= update.Count {
			continue
		}

		d.UpdateTopList(update)

	}

}

// func BinarySearchDecending(list []Details, item Details, fn func(l, r Details) int) (index int, found bool) {
// 	// fmt.Println("starting new")
// 	return binarySearchDecending(list, item, fn, 0)
// }

// func binarySearchDecending(list []Details, item Details, fn func(l, r Details) int, offset int) (index int, found bool) {

// 	if len(list) == 0 {
// 		return offset + 1, false
// 	}

// 	m := len(list) / 2

// 	res := fn(item, list[m])

// 	// fmt.Printf("list: %v, %v, %v, %v\n", list, item, offset, res)

// 	if res > 0 {

// 		if len(list) == 1 {
// 			return offset, false
// 		}
// 		// lhs [0:m]
// 		return binarySearchDecending(list[:m], item, fn, offset)
// 	} else if res < 0 {
// 		if len(list) == 1 {
// 			return offset + 1, false
// 		}
// 		// rhs [m:len(list)]
// 		return binarySearchDecending(list[m:], item, fn, m+offset)
// 	}
// 	return offset + m, true
// }

func idxDescending(list []Details, d Details) int {
	lo, hi := 0, len(list)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if list[mid].Count < d.Count {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func main() {
	dataStream := NewDataStreamFanout()

	msgs := dataStream.PrepareData()

	dataStream.Setup()

	start := time.Now()

	dataStream.Input(msgs)

	dataStream.Close()

	fmt.Println("Took: ", time.Now().Sub(start))

	// time.Sleep(30 * time.Second)
}
