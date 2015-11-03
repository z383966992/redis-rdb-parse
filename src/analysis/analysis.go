package analysis

/**
@author: zhouliangliang5
*/
import (
	"container/heap"
	"container/list"
	"dbutil"
	"encoding/json"
	"logs"
	"sorting"
	"strings"
)

type ValueTypeStat struct {
	ValueType string
	Num       int
}

type ValueLengthFilterData struct {
	Key         string
	ValueLength int
}

func Analysis(typs int, resultMap *map[string]int,
	key string, valueLength int, filterLength int, valueLengthFilterList *list.List, filterKey string, keyFilterMap *map[string]int, heap *sorting.Heap, heapInit *bool) {

	//这一个用来分析每一个value类型的数量
	valueTypeStatistics(typs, resultMap)

	//这一个用来分析value length大于某个值的key
	//	valueLengthFilter(key, valueLength, filterLength, valueLengthFilterList)
	//堆排序获取前N个值
	HeapSort(key, valueLength, filterLength, heap, heapInit)
	//key过滤
	if filterKey != "" {
		KeyFilter(key, valueLength, filterKey, keyFilterMap)
	}
}

//****************************************************************************************
//过滤key
func KeyFilter(key string, valueLength int, filterKey string, keyMap *map[string]int) {
	//首先截取key
	keys := strings.Split(filterKey, ",")
	for _, content := range keys {
		if strings.Contains(strings.ToUpper(key), strings.ToUpper(content)) {
			(*keyMap)[key] = valueLength
		}
	}
}

//输出过滤的key
func StoreKeyFiltResult(keyMap map[string]int, taskId string) {

	for key, valueL := range keyMap {
		var value ValueLengthFilterData
		value.Key = key
		value.ValueLength = valueL
		b, err := json.Marshal(value)

		if err != nil {
			logs.Log("json marshal error in valueLengthFilter : " + err.Error())
			continue
		}
		dbutil.Store(string(b), "3", taskId)
	}
}

//****************************************************************************************
//堆排序
func HeapSort(key string, valueLength int, num int, heap *sorting.Heap, heapInit *bool) {
	var elem sorting.Element
	elem.Key = key
	elem.Length = valueLength
	sorting.Sort(heap, elem, num, heapInit)
}

//存储堆排序结果
func StoreSortingResult(h *sorting.Heap, taskId string) {
	for h.Len() > 0 {
		element := heap.Pop(h).(sorting.Element)
		b, err := json.Marshal(element)
		if err != nil {
			logs.Log("json marshal error in StoreValueTypeStat : " + err.Error())
			return
		}
		dbutil.Store(string(b), "2", taskId)
	}
}

//****************************************************************************************

//value length 过滤，用来获得value length 大于某个值的key
func valueLengthFilter(key string, valueLength int, filterLength int, valueLengthFilterList *list.List) {

	if valueLength > filterLength {
		var value ValueLengthFilterData
		value.Key = key
		value.ValueLength = valueLength
		b, err := json.Marshal(value)

		if err != nil {
			logs.Log("json marshal error in valueLengthFilter : " + err.Error())
			return
		}
		valueLengthFilterList.PushBack(string(b))
	}
}

//计算每一个值类型的个数
func valueTypeStatistics(types int, resultMap *map[string]int) {

	//值类型统计
	switch types {
	case 0: //value type String Encoding
		calNum("value type String Encoding", resultMap)
	case 1: //value type List Encoding
		calNum("value type List Encoding", resultMap)
	case 2: //value type Set Encoding
		calNum("value type Set Encoding", resultMap)
	case 3: //value type Sorted Set Encoding
		calNum("value type Sorted Set Encoding", resultMap)
	case 4: //value type Hash Encoding
		calNum("value type Hash Encoding", resultMap)
	case 9: //value type Zipmap Encoding
		calNum("value type Zipmap Encoding", resultMap)
	case 10: //value type Ziplist Encoding
		calNum("value type Ziplist Encoding", resultMap)
	case 11: //value type Intset Encoding
		calNum("value type Intset Encoding", resultMap)
	case 12: //value type Sorted Set In Ziplist Encoding
		calNum("value type Sorted Set In Ziplist Encoding", resultMap)
	case 13: //value type HashMap In Ziplist Encoding
		calNum("value type HashMap In Ziplist Encoding", resultMap)
	}
}

//计算过程
func calNum(value string, resultMap *map[string]int) {
	v, ok := (*resultMap)[value]
	if ok {
		v = v + 1
		(*resultMap)[value] = v
	} else {
		(*resultMap)[value] = 1
	}
}

func StoreValueTypeStat(resultMap map[string]int, fileId string) {
	for k, v := range resultMap {
		var value ValueTypeStat
		value.ValueType = k
		value.Num = v
		b, err := json.Marshal(value)
		if err != nil {
			logs.Log("json marshal error in StoreValueTypeStat : " + err.Error())
			return
		}
		dbutil.Store(string(b), "1", fileId)
	}
}

func StoreValueLengthFilter(list *list.List, fileId string) {

	for e := list.Front(); e != nil; e = e.Next() {
		str, ok := e.Value.(string)
		if ok {
			dbutil.Store(str, "2", fileId)
		} else {
			logs.Log("list element error in StoreValueLengthFilter list element is not string!")
			continue
		}
	}
}

//func OutPut(h *sorting.Heap) {
//	for h.Len() > 0 {
//		element := heap.Pop(h).(sorting.Element)
//		//		logs.Log("Key : " + element.Key + " Length : " + strconv.FormatInt(int64(element.Length), 10))
//		fmt.Printf("%d %s \n", element.Length, element.Key)
//	}
//}
