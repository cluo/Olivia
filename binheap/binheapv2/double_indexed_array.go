package binheapv2

import (
	"fmt"
	. "github.com/GrappigPanda/Olivia/binheap"
	"sync"
	"time"
)

type Direction int

const (
	INCREMENT Direction = iota
	DECREMENT
)

type BinheapOptimized struct {
	Tree      []*Node
	maxIndex  int
	minIndex  int
	keyLookup map[string]int
	sync.Mutex
}

func NewBinheapOptimized(maxSize int) *BinheapOptimized {
	return &BinheapOptimized{
		maxIndex:  0,
		minIndex:  0,
		Tree:      make([]*Node, maxSize),
		keyLookup: make(map[string]int),
	}
}

func (d *BinheapOptimized) Copy() *BinheapOptimized {
	d.Lock()

	newStorage := NewBinheapOptimized(len(d.Tree))

	for index, element := range d.Tree {
		newStorage.Tree[index] = element
	}

	for k, v := range d.keyLookup {
		newStorage.keyLookup[k] = v
	}

	newStorage.maxIndex = d.maxIndex
	newStorage.minIndex = d.minIndex

	d.Unlock()

	return newStorage
}

func (d *BinheapOptimized) MinNode() *Node {
	return d.Tree[d.maxIndex]
}

func (d *BinheapOptimized) Insert(newNode *Node) *Node {
	d.Lock()

	if d.IsEmpty() {
		d.insertAtIndexZero(newNode)

		d.Unlock()
		return newNode
	} else if d.IsFull() {
		d.evictMinNodeLockless()

		if d.IsEmpty() {
			d.insertAtIndexZero(newNode)

			d.Unlock()
			return newNode
		}
	}

	nextIndex := 0
	if compareTimeouts(d.Tree[d.maxIndex].Timeout, newNode.Timeout) {
		nextIndex = safeIndex(cap(d.Tree), d.minIndex, DECREMENT)
		d.minIndex = nextIndex
	} else {
		nextIndex = safeIndex(cap(d.Tree), d.maxIndex, INCREMENT)
		d.maxIndex = nextIndex
	}

	d.Tree[nextIndex] = newNode
	d.keyLookup[newNode.Key] = nextIndex

	d.Unlock()
	return newNode
}

func (d *BinheapOptimized) insertAtIndexZero(newNode *Node) *Node {
	d.Tree[0] = newNode
	d.maxIndex = 0
	d.minIndex = 0
	d.keyLookup[newNode.Key] = 0

	return newNode
}

func (d *BinheapOptimized) EvictMinNode() *Node {
	d.Lock()
	minNode := d.evictMinNodeLockless()
	d.Unlock()

	return minNode
}

func (d *BinheapOptimized) evictMinNodeLockless() *Node {
	minNode := d.Tree[d.minIndex]

	d.Tree[d.minIndex] = nil

	nextIndex := 0
	if !d.IsEmpty() {
		nextIndex = safeIndex(cap(d.Tree), d.minIndex, INCREMENT)
	}
	d.minIndex = nextIndex
	delete(d.keyLookup, minNode.Key)

	return minNode
}

// Peek handles looking at the index of the tree.
func (d *BinheapOptimized) Peek(index int) (*Node, error) {
	if index > d.CurrentSize() {
		return nil, fmt.Errorf("Index greater than size of heap.")
	}
	return d.Tree[index], nil
}

func (d *BinheapOptimized) IsEmpty() bool {
	return d.maxIndex == d.minIndex && d.Tree[d.maxIndex] == nil
}

func (d *BinheapOptimized) IsFull() bool {
	return d.maxIndex == d.minIndex && d.Tree[d.maxIndex] != nil && d.Tree[d.minIndex] != nil
}

// ReAllocate Handles increasing the size of the underlying binary heap.
func (d *BinheapOptimized) ReAllocate(maxSize int) {
	// TODO(ian): If `maxSize` decreases, we should do something!
	d.Tree = append(d.Tree, make([]*Node, maxSize)...)
}

// UpdateNodeTimeout allows changing of the keys Timeout in the
func (d *BinheapOptimized) UpdateNodeTimeout(key string) *Node {
	d.Lock()
	nodeIndex, ok := d.keyLookup[key]
	if !ok {
		return nil
	}

	d.Tree[nodeIndex].Timeout = time.Now().UTC()

	if nodeIndex+1 < d.CurrentSize() {
		// TODO(ian): Finish these percolation methods.
		if d.compareTwoTimes(nodeIndex, nodeIndex+1) {
			// d.percolateDown(nodeIndex)
		} else if d.compareTwoTimes(nodeIndex-1, nodeIndex) {
			// d.percolateUp(nodeIndex)
		}
	}

	node, _ := d.Get(key)

	d.Unlock()
	return node
}

// Get handles retrieving a Node by its key. Not extensively used, but it was a
// nice-to-have.
func (d *BinheapOptimized) Get(key string) (*Node, bool) {
	if index, ok := d.keyLookup[key]; ok {
		return d.Tree[index], ok
	} else {
		return nil, ok
	}
}

func (d *BinheapOptimized) CurrentSize() int {
	// NOTE: The only time the dereferenced value at d.maxIndex is nil is whenever the binheap is empty.
	if d.Tree[d.maxIndex] != nil {
		// NOTE: If we wrap around the array, we nee to handle that.
		if d.maxIndex < d.minIndex {
			return d.maxIndex + 1 + (cap(d.Tree))
		} else {
			return d.maxIndex - d.minIndex
		}
	} else {
		return 0
	}
}

// compareTwoTimes takes two indexes and compares the `.Nanosecond()` value of
// each in the tree. If the left (i) has an expiration time _after_ the right
// (j), then we return True. Otherwise, if the left (i) has an expiration time
// _before_ the right (j) we return a False.
func (h *BinheapOptimized) compareTwoTimes(i int, j int) bool {
	return compareTimeouts(h.Tree[i].Timeout, h.Tree[j].Timeout)
}

// swapTwoNodes swaps j into i and vice versa. Moreover, it handles updating
// the keyLookup field in the heap so that we can continue to quickly retrieve
// key Timeouts.
func (d *BinheapOptimized) swapTwoNodes(i int, j int) {
	// If we find a value at Tree[i], we can update it in the keylookup,
	// otherwise disregard, as it's a recently evicted node.
	if d.Tree[i] != nil {
		d.keyLookup[d.Tree[i].Key] = j
	}
	if d.Tree[j] != nil {
		d.keyLookup[d.Tree[j].Key] = i
	}

	d.Tree[j], d.Tree[i] = d.Tree[i], d.Tree[j]
}

func compareTimeouts(time1 time.Time, time2 time.Time) bool {
	return time1.Sub(time2) > 0
}

func safeIndex(treeCapacity, i int, direction Direction) int {
	if direction == INCREMENT {
		nextVal := i + 1
		if nextVal > treeCapacity {
			return nextVal % treeCapacity
		} else {
			return nextVal
		}
	} else {
		nextVal := i - 1
		if nextVal < 0 {
			return treeCapacity - (nextVal * -1)
		} else {
			return nextVal
		}
	}
}
