package binheapv2

import (
	"fmt"
	. "github.com/GrappigPanda/Olivia/binheap"
	"testing"
	"time"
)

func TestSafeIndexIncrement(t *testing.T) {
	retval := safeIndex(100, 5, INCREMENT)

	if retval != 6 {
		t.Errorf("Expected %v, got %v", 6, retval)
	}
}

func TestSafeIndexDecrement(t *testing.T) {
	retval := safeIndex(100, 5, DECREMENT)

	if retval != 4 {
		t.Errorf("Expected %v, got %v", 4, retval)
	}
}

func TestSafeIndexDecrementToZero(t *testing.T) {
	retval := safeIndex(100, 1, DECREMENT)

	if retval != 0 {
		t.Errorf("Expected %v, got %v", 0, retval)
	}
}

func TestSafeIndexIncrementOffZero(t *testing.T) {
	retval := safeIndex(100, 0, INCREMENT)

	if retval != 1 {
		t.Errorf("Expected %v, got %v", 1, retval)
	}
}

func TestIsEmpty(t *testing.T) {
	testHeap := NewBinheapOptimized(10)

	if testHeap.IsEmpty() != true {
		t.Errorf("Expected an empty heap, got %v", testHeap)
	}
}

func TestIsEmptyHasNode(t *testing.T) {
	testHeap := NewBinheapOptimized(1)
	testNode := NewNode("Testswap", time.Now().UTC())

	testHeap.Insert(testNode)

	if testHeap.IsEmpty() == true {
		t.Errorf("Expected a non-empty heap, got %v", testHeap)
	}
}

func TestPeek(t *testing.T) {
	testHeap := NewBinheapOptimized(10)
	testNode := NewNode("TestingNewNodeKey", time.Now().UTC())

	testHeap.Insert(testNode)

	retval, err := testHeap.Peek(0)
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}

	if retval.Key != "TestingNewNodeKey" {
		t.Errorf("Expected %v, got %v", testNode.Key, retval.Key)
	}
}

func TestPeekFailIndexTooLarge(t *testing.T) {
	testHeap := NewBinheapOptimized(10)
	testNode := NewNode("TestingNewNodeKey", time.Now().UTC())

	testHeap.Insert(testNode)

	retval, err := testHeap.Peek(4)
	if err == nil {
		t.Errorf("Expected non-nil error, got nil -- %v", retval)
	}
}

func TestNewNode(t *testing.T) {
	expectedReturn := Node{
		Key:     "TestingNewNodeKey",
		Timeout: time.Now().UTC(),
	}

	time.Sleep(5 * time.Millisecond)
	retVal := NewNode("TestingNewNodeKey", time.Now().UTC())

	if expectedReturn.Key != retVal.Key {
		t.Errorf("Expected %v, got %v", expectedReturn.Key, retVal.Key)
	}

	if expectedReturn.Timeout.Sub(retVal.Timeout) > 0 {
		t.Errorf("Expected expectedReturn (%v) to be lower than retval (%v)",
			expectedReturn.Timeout,
			retVal.Timeout,
		)
	}
}

func TestNewBinheap(t *testing.T) {
	expectedReturn := BinheapOptimized{
		Tree: make([]*Node, 10),
	}

	retVal := NewBinheapOptimized(10)

	if expectedReturn.maxIndex != retVal.maxIndex {
		t.Errorf("Expected %v, got %v", expectedReturn.maxIndex, retVal.maxIndex)
	}

	if len(expectedReturn.Tree) != len(retVal.Tree) {
		t.Errorf("Expected %v, got %v", len(expectedReturn.Tree), len(retVal.Tree))
	}
}

func TestHeapInsertEmptyTree(t *testing.T) {
	testHeap := NewBinheapOptimized(1)
	testNode := NewNode("TestHeapInsertEmptyTree", time.Now().UTC())
	time.Sleep(5 * time.Millisecond)

	testHeap.Insert(testNode)

	if testHeap.Tree[0].Key != testNode.Key {
		t.Errorf("Node didn't insert into index 0 for an empty tree",
			testHeap,
		)
	}
}

func TestHeapInsertOverflow(t *testing.T) {
	testHeap := NewBinheapOptimized(1)
	testNode := NewNode("TestHeapInsertOverflow", time.Now().UTC())
	time.Sleep(5 * time.Millisecond)
	testNode2 := NewNode("TestHeapInsertOverflow2", time.Now().UTC())

	testHeap.Insert(testNode)
	testHeap.Insert(testNode2)

	if _, ok := testHeap.keyLookup[testNode2.Key]; !ok {
		t.Errorf("Failed to find %v in keylookup after overwrite",
			testHeap.Tree[0].Key,
		)
	}

	if testHeap.Tree[0].Key != testNode2.Key {
		t.Errorf("Incorrect overflow--didn't overwrite index 0. Expected %v, got %v",
			testHeap.Tree[0].Key,
			testNode.Key,
		)
	}
}

func TestInsertAndMinNode(t *testing.T) {
	testHeap := NewBinheapOptimized(10)
	testNode := NewNode("TestHeapInsert", time.Now().UTC())

	testHeap.Insert(testNode)

	if testHeap.MinNode().Key != "TestHeapInsert" {
		t.Errorf("Failed retrieving min node, got back %v. Tree: %v",
			testHeap.MinNode().Key,
			testHeap.Tree,
		)
	}
}

func TestMinNodeFailNoRootNode(t *testing.T) {
	testHeap := NewBinheapOptimized(1)

	if testHeap.MinNode() != nil {
		t.Errorf("Expected nil, got %v with a heap of %v",
			testHeap.MinNode(),
			testHeap,
		)
	}
}

func TestSwap(t *testing.T) {
	testHeap := NewBinheapOptimized(5)
	testNode := NewNode("Testswap", time.Now().UTC())
	time.Sleep(5 * time.Millisecond)
	testNode2 := NewNode("Testswap2", time.Now().UTC())

	testHeap.Insert(testNode)
	testHeap.Insert(testNode2)

	minNode := testHeap.MinNode()

	testHeap.swapTwoNodes(0, 1)

	newMinNode := testHeap.MinNode()

	if minNode == newMinNode {
		t.Errorf("Expected nodes to swap: MinNode %v - NewMinNode %v -  Heap %v",
			minNode,
			newMinNode,
			testHeap,
		)
	}
}

func TestKeyLookup(t *testing.T) {
	testHeap := NewBinheapOptimized(5)
	testNode1 := NewNode("TestNode1", time.Now().UTC())
	time.Sleep(5 * time.Millisecond)
	testNode2 := NewNode("TestNode2", time.Now().UTC())
	time.Sleep(5 * time.Millisecond)
	testNode3 := NewNode("TestNode3", time.Now().UTC())

	testHeap.Insert(testNode1)
	testHeap.Insert(testNode2)
	testHeap.Insert(testNode3)

	node1Index := testHeap.keyLookup[testNode1.Key]
	node2Index := testHeap.keyLookup[testNode2.Key]
	node3Index := testHeap.keyLookup[testNode3.Key]

	if node1Index != 2 {
		t.Errorf("Incorrect index for node1 %v", node1Index)
	}

	if node2Index != 1 {
		t.Errorf("Incorrect index for node2 %v", node2Index)
	}

	if node3Index != 0 {
		t.Errorf("Incorrect index for node3 %v", node3Index)
	}
}

func TestKeyLookupIndexesProperly(t *testing.T) {
	testHeap := NewBinheapOptimized(25)

	keyValues := make([]string, 24)
	for i := 0; i < 24; i++ {
		keyName := fmt.Sprintf("Node-%v", i)
		testNode := NewNode(keyName, time.Now().UTC())
		keyValues[i] = keyName
		testHeap.Insert(testNode)
		time.Sleep(5 * time.Millisecond)
	}

	for i := 0; i < 24; i++ {
		key := keyValues[i]
		keyIndex := testHeap.keyLookup[key]

		if keyIndex != i {
			for i := range keyValues {
				t.Errorf(keyValues[i])
			}
			return
			t.Errorf("Expected key %v to have an index of %v but had index of %v",
				key,
				i,
				keyIndex,
			)
		}
	}
}

func TestKeyLookupReadjustsOnEviction(t *testing.T) {
	testHeap := NewBinheapOptimized(25)

	keyValues := make([]string, 24)
	for i := 0; i < 24; i++ {
		keyName := fmt.Sprintf("Node-%v", i)
		testNode := NewNode(keyName, time.Now().UTC())
		keyValues[i] = keyName
		testHeap.Insert(testNode)
		time.Sleep(5 * time.Millisecond)
	}

	testHeap.EvictMinNode()

	for i := 0; i < 24; i++ {
		if i == 0 {
			continue
		}

		key := keyValues[i]
		keyIndex := testHeap.keyLookup[key]

		if keyIndex != i-1 {
			t.Errorf("Expected key %v to have an index of %v but had index of %v",
				key,
				keyIndex-1,
				keyIndex,
			)
		}
	}
}

func TestKeyLookupReadjustsOnInsertion(t *testing.T) {
	testHeap := NewBinheapOptimized(25)

	originalNode := NewNode("OriginalNode", time.Now().UTC())
	time.Sleep(50 * time.Millisecond)

	keyValues := make([]string, 24)
	for i := 0; i < 24; i++ {
		keyName := fmt.Sprintf("Node-%v", i)
		testNode := NewNode(keyName, time.Now().UTC())
		keyValues[i] = keyName
		testHeap.Insert(testNode)
		time.Sleep(5 * time.Millisecond)
	}

	testHeap.Insert(originalNode)

	for i := 0; i < 24; i++ {
		if i == 0 {
			continue
		}

		key := keyValues[i]
		keyIndex := testHeap.keyLookup[key]

		if keyIndex != i+1 {
			t.Errorf("Expected key %v to have an index of %v but had index of %v",
				key,
				keyIndex+1,
				keyIndex,
			)
		}
	}
}

func TestKeyUpdateTimeoutDoesntBlowUpEverything(t *testing.T) {
	testHeap := NewBinheapOptimized(25)

	keyValues := make([]string, 25)
	for i := 0; i < 25; i++ {
		keyName := fmt.Sprintf("Node-%v", i)
		testNode := NewNode(keyName, time.Now().UTC())
		keyValues[i] = keyName
		testHeap.Insert(testNode)
		time.Sleep(5 * time.Millisecond)
	}

	ok := testHeap.UpdateNodeTimeout(keyValues[3])
	if ok == nil {
		t.Errorf("Got weird error, %v index %v", keyValues, 3)
	}

	for i := 0; i < len(testHeap.Tree)-1; i++ {
		for j := i + 1; j < len(testHeap.Tree)-1; j++ {
			if testHeap.compareTwoTimes(i, j) {
				t.Errorf(
					"%v - %v -- %v - %v",
					testHeap.Tree[i].Key,
					testHeap.Tree[i].Timeout,
					testHeap.Tree[j].Key,
					testHeap.Tree[j].Timeout,
				)
				break
			}
		}
	}
}

func TestCopy(t *testing.T) {
	testHeap := NewBinheapOptimized(10)

	keyValues := make([]string, 10)
	for i := 0; i < 10; i++ {
		keyName := fmt.Sprintf("Node-%v", i)
		testNode := NewNode(keyName, time.Now().UTC())
		keyValues[i] = keyName
		testHeap.Insert(testNode)
		time.Sleep(5 * time.Millisecond)
	}

	copyHeap := testHeap.Copy()

	for i := 0; i < 10; i++ {
		if copyHeap.Tree[i] != testHeap.Tree[i] {
			t.Errorf("Expected %v, got %v", testHeap.Tree[i], copyHeap.Tree[i])
		}
	}
}

func TestGet(t *testing.T) {
	testHeap := NewBinheapOptimized(10)
	testNode := NewNode("TestingNewNodeKey", time.Now().UTC())

	testHeap.Insert(testNode)

	retval, keyExists := testHeap.Get(testNode.Key)
	if !keyExists {
		t.Errorf("Expected %v, got %v", testNode, retval)
	}
}

func TestGetFailInvalidKey(t *testing.T) {
	testHeap := NewBinheapOptimized(10)
	testNode := NewNode("TestingNewNodeKey", time.Now().UTC())

	testHeap.Insert(testNode)

	retval, keyExists := testHeap.Get("INVALID_KEY")
	if keyExists {
		t.Errorf("Expected no key found, got %v", retval)
	}
}

func TestCurrentSize(t *testing.T) {
	testHeap := NewBinheapOptimized(10)
	testNode := NewNode("TestingNewNodeKey", time.Now().UTC())

	testHeap.Insert(testNode)
	testHeap.Insert(testNode)
	testHeap.Insert(testNode)

	retval := testHeap.CurrentSize()
	if retval != 3 {
		t.Errorf("Expected 3, got %v", retval)
	}
}

func TestCurrentSizeEmpty(t *testing.T) {
	testHeap := NewBinheapOptimized(10)

	retval := testHeap.CurrentSize()
	if retval != 0 {
		t.Errorf("Expected 0, got %v", retval)
	}
}
