package sorting

import (
	"container/heap"
)

type Element struct {
	Length int
	Key    string
}

// An IntHeap is a min-heap of ints.
type Heap []Element

func (h Heap) Len() int           { return len(h) }
func (h Heap) Less(i, j int) bool { return h[i].Length < h[j].Length }
func (h Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *Heap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(Element))
}

func (h *Heap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func Sort(h *Heap, element Element, num int, heapInit *bool) {

	//init只能执行一次
	if (*heapInit) == false {
		heap.Init(h)
		(*heapInit) = true
	}

	if num > 0 {
		if h.Len() >= num && h.Len() != 0 {
			elem := heap.Pop(h).(Element)
			if elem.Length > element.Length {
				heap.Push(h, elem)
			} else {
				heap.Push(h, element)
			}
		} else {
			heap.Push(h, element)
		}
	}

}

// This example inserts several ints into an IntHeap, checks the minimum,
// and removes them in order of priority.
//func Example_intHeap() {
//	var element1 Element
//	element1.Length = 10
//	element1.Key = "a"
//
//	var element2 Element
//	element2.Length = 8
//	element2.Key = "b"
//
//	var element3 Element
//	element3.Length = 11
//	element3.Key = "c"
//
//	var element4 Element
//	element4.Length = 20
//	element4.Key = "d"
//
//	h := &Heap{element1, element2, element3}
//	//	heap.Init(h)
//	//	h.Push(element4)
//	heap.Push(h, element4)
//	fmt.Printf("minimum: %d   %s\n", (*h)[0].Length, (*h)[0].Key)
//	for h.Len() > 0 {
//		element := heap.Pop(h).(Element)
//		fmt.Printf("%d %s \n", element.Length, element.Key)
//	}
//}
