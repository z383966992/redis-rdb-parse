package parse

/**
@author: zhouliangliang5
*/
import (
	"analysis"
	"bytes"
	"config"
	"container/list"
	"encoding/binary"
	"fmt"
	"io"
	"logs"
	"lzf"
	"os"
	"sorting"
	"strconv"
	"time"
)

type Parameters struct {
	//解析成功还是失败
	Success bool
	//解析用的index
	SliceStart uint64
	SliceEnd   uint64

	//解析固定信息的时候用的
	Step []uint64

	//用来测试输出文件是否存在
	FileFlag bool

	//分块读取当中管道的大小
	//1024代表1kb
	SliceContent uint64

	//固定信息是否解析过，在分段解析当中，固定信息不能重复解析
	FixedInfo bool

	//下次读取数据从哪开始
	OffsetBytes int64

	//记录解析的数据库
	Dbnum int

	//以下都是分析用的变量
	ResultMap             map[string]int
	ValueFilterLength     int
	FilterKey             string
	ValueLengthFilterList *list.List
	KeyFilterMap          map[string]int
	Heap                  *sorting.Heap
	HeapInit              *bool
	//key总数
	TotalNum int64
}

/**
这个是解析的入口
pwflag 表示是打印还是写入到文件
0 打印
1 写入到文件
2 写入到数据库
filename 表示要解析的文件路径
*/
func Start(inputFile string, filterLength int, filterKey string) Parameters {

	//读取管道的大小
	conf := config.SetConfig("important.properties")
	sliceContent, _ := strconv.ParseUint(conf.GetValue("pipeline", "size"), 10, 64)
	if sliceContent < 16 {
		sliceContent = 16
		logs.Log("sliceContent < 16")
	}

	var heapIni bool = false

	var param Parameters
	param.Success = false
	param.SliceStart = 0
	param.SliceEnd = 0
	param.Step = []uint64{5, 4}
	param.FileFlag = false
	param.SliceContent = sliceContent
	param.FixedInfo = false
	param.OffsetBytes = 0
	param.Dbnum = -1
	param.ResultMap = make(map[string]int)
	param.ValueFilterLength = filterLength
	param.FilterKey = filterKey
	param.ValueLengthFilterList = list.New()
	param.KeyFilterMap = make(map[string]int)
	param.Heap = &sorting.Heap{}
	param.HeapInit = &heapIni
	pipelineAnalyze(inputFile, &param)

	return param
}

//管道读取
func pipelineAnalyze(inputFile string, param *Parameters) {
	file, err := os.Open(inputFile)

	if err != nil && err != io.EOF {
		logs.Log("file open error in parseRdb pipelineAnalyze : " + err.Error())
		return
	}

	defer file.Close()

	//首先读取一定数量的字节
	buff := make([]byte, (*param).SliceContent)

	n, err := file.Read(buff)
	if err != nil && err != io.EOF {
		logs.Log("file read error in parseRdb pipelineAnalyze : " + err.Error())
		return
	}

	//解析
	var pbuff = &buff
	if n != 0 {
		(*param).OffsetBytes = (*param).OffsetBytes + int64(n)
		parseRedisRdb(pbuff, inputFile, param)
		(*param).FixedInfo = true
	}
	//为了防止文件没有全部读取，再次循环读取,理论上这个地方应该用不到
	for {
		n, err := file.ReadAt(buff, (*param).OffsetBytes)

		if err != nil && err != io.EOF {
			logs.Log("file read error in parseRdb pipelineAnalyze for loop : " + err.Error())
			return
		}

		defer file.Close()

		if n == 0 {
			break
		} else {
			(*param).SliceStart = 0
			(*param).SliceEnd = (*param).SliceStart + 1
			(*param).OffsetBytes = (*param).OffsetBytes + int64(n)
			parseRedisRdb(pbuff, inputFile, param)
			(*param).FixedInfo = true
		}
	}
}

/**
接受一个redis rdb文件的byte数组，并解析
List Encoding 与 Set Encoding 与 Sorted Set Enconding 的编码方式是一样的
Sorted Set in Ziplist Encoding 与 Ziplist Encoding 的编码方式是一样的
Zipmap encoding are deprecated starting Redis 2.6. Small hashmaps are now encoded using ziplists.
pwflag用来判断是打印还是输出到文件中
*/
func parseRedisRdb(array *[]byte, inputFile string, param *Parameters) {
	//解析固定信息
	//固定信息只能解析一次
	if (*param).FixedInfo == false {
		parseFixedInformation(array, param)
	}
	defer func() {
		if r := recover(); r != nil {
			//设置解析失败
			(*param).Success = false
			//记录log
			logs.Log(r)
		}
	}()
	var flag bool = true

	for flag {
		reIndex(array, inputFile, 1, param)
		slice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array)
		var i uint8 = uint8(slice[0])

		if i == 255 {
			break
		}

		switch i {

		case 0:
			(*param).TotalNum++
			key, _ := parseKeyString(array, inputFile, param)
			value, length := parseStringEncodingValue(array, inputFile, param)
			str := "value type String Encoding: " + key + "--->" + value
			dealResult(key, length, str, 0, param)
		case 1:
			(*param).TotalNum++
			key, _ := parseKeyString(array, inputFile, param)
			length := parseListEncodingValue(array, inputFile, param)
			str := "value type List Encoding: " + key + "--->" + strconv.FormatInt(int64(length), 10)
			dealResult(key, length, str, 1, param)
		case 2:
			(*param).TotalNum++
			key, _ := parseKeyString(array, inputFile, param)
			length := parseSetEncodingValue(array, inputFile, param)
			str := "value type Set Encoding: " + key + "--->" + strconv.FormatInt(int64(length), 10)
			dealResult(key, length, str, 2, param)
		case 3:
			(*param).TotalNum++
			key, _ := parseKeyString(array, inputFile, param)
			length := parseSortedSetEncodingValue(array, inputFile, param)
			str := "value type Sorted Set Encoding: " + key + "--->" + strconv.FormatInt(int64(length), 10)
			dealResult(key, length, str, 3, param)
		case 4:
			(*param).TotalNum++
			key, _ := parseKeyString(array, inputFile, param)
			length := parseHashEncodingValue(array, inputFile, param)
			str := "value type Hash Encoding: " + key + "--->" + strconv.FormatInt(int64(length), 10)
			dealResult(key, length, str, 4, param)

		case 9: //2.6以前的版本有这个
			(*param).TotalNum++
			key, _ := parseKeyString(array, inputFile, param)
			length := parseZipMapEncodingValue(array, inputFile, param)
			str := "value type Zipmap Encoding: " + key + "--->" + strconv.FormatInt(int64(length), 10)
			dealResult(key, length, str, 9, param)
		case 10:
			(*param).TotalNum++
			key, _ := parseKeyString(array, inputFile, param)
			length := parseZiplistEncodingValue(array, inputFile, param)
			str := "value type Ziplist Encoding: " + key + "--->" + strconv.FormatInt(int64(length), 10)
			dealResult(key, length, str, 10, param)
		case 11:
			(*param).TotalNum++
			key, _ := parseKeyString(array, inputFile, param)
			length := parseIntSetEncoding(array, inputFile, param)
			str := "value type Intset Encoding: " + key + "--->" + strconv.FormatInt(int64(length), 10)
			dealResult(key, length, str, 11, param)
		case 12:
			(*param).TotalNum++
			key, _ := parseKeyString(array, inputFile, param)
			length := parseZiplistEncodingValue(array, inputFile, param)
			str := "value type Sorted Set In Ziplist Encoding: " + key + "--->" + strconv.FormatInt(int64(length), 10)
			dealResult(key, length, str, 12, param)
		case 13:
			(*param).TotalNum++
			key, _ := parseKeyString(array, inputFile, param)
			length := parseHashMapInZiplistEncodingValue(array, inputFile, param)
			str := "value type HashMap In Ziplist Encoding: " + key + "--->" + strconv.FormatInt(int64(length), 10)
			dealResult(key, length, str, 13, param)
		case 252: //后边跟的有效期是毫秒(ms)
			parseExpireTimeInMillSeconds(array, inputFile, param)
		case 253: //后边跟的是有效期是秒(s)
			parseExpireTimeInSeconds(array, inputFile, param)
		case 254: //redis 数据库编号
			parseRedisDBNum(array, inputFile, param)
		default:
			logs.Log("error occurred in parseFunc : " + strconv.FormatInt(int64(i), 10))
			(*param).Success = false
			return
		}
	}
	(*param).Success = true
}

/**
0 打印
1 将结果写入到文件中
2 将结果写入到数据库中
*/
func dealResult(key string, length int, str string, types int, param *Parameters) {

	analysis.Analysis(types, &(*param).ResultMap,
		key, length, (*param).ValueFilterLength, (*param).ValueLengthFilterList, (*param).FilterKey, &(*param).KeyFilterMap, (*param).Heap, (*param).HeapInit)
}

//此方法用来组装key或者value对应的String
func assembelString(array []byte) string {
	return string(array)
}

//给定一个数组，和一个开始，截止位置，返回一个数组切片
func getArraySlice(start uint64, end uint64, array *[]byte) []byte {
	return (*array)[start:end]
}

//给定一个byte，获得byte[0]前两个bit的内容和后6个bit代表的数值
func calByte(byValue byte) (pre uint8, suf uint8) {
	//前缀值
	var prefixValue uint8 = uint8(byValue & 0xC0)
	//后缀值
	var suffixValue uint8 = uint8(byValue & 0x3F)
	return prefixValue, suffixValue
}

//从array当中读取下num个字节
func readNextNByte(array *[]byte, inputFile string, num uint64, param *Parameters) []byte {
	reIndex(array, inputFile, num, param)
	slice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array)
	return slice
}

//重新定位sliceStart和sliceEnd的位置
//并且当管道溢出的时重新读取数据
func reIndex(array *[]byte, inputFile string, num uint64, param *Parameters) {
	(*param).SliceStart = (*param).SliceEnd
	(*param).SliceEnd = (*param).SliceEnd + num

	checkOverFlow(array, inputFile, num, false, param)
}

//重新定位sliceEnd的位置
//并且当管道溢出的时重新读取数据
func reIndexSliceEnd(array *[]byte, inputFile string, num uint64, param *Parameters) {
	(*param).SliceEnd = (*param).SliceEnd + num
	checkOverFlow(array, inputFile, num, true, param)
}

func checkOverFlow(array *[]byte, inputFile string, num uint64, flag bool, param *Parameters) {

	//此时管道溢出了,需要重新读取数据
	if (*param).SliceEnd > (*param).SliceContent {
		*array = (*array)[(*param).SliceStart:]
		//1.首先要计算读取多少数据

		var temp []byte
		if (*param).SliceEnd-(*param).SliceContent < (*param).SliceStart {
			temp = make([]byte, (*param).SliceStart)
		} else {
			//如果读取的数据大于管道的大小
			temp = make([]byte, ((*param).SliceEnd-(*param).SliceContent)+1024)
		}

		file, err := os.Open(inputFile)

		if err != nil {
			logs.Log("file open error in checkOverFlow : " + err.Error())
			return
		}

		defer file.Close()

		n, err := file.ReadAt(temp, (*param).OffsetBytes)

		if err != nil && err != io.EOF {
			logs.Log("file read error in checkOverFlow : " + err.Error())
			return
		}

		if n != 0 {
			(*param).OffsetBytes = (*param).OffsetBytes + int64(n)
			*array = append(*array, temp...)
		}
		(*param).SliceEnd = (*param).SliceEnd - (*param).SliceStart
		(*param).SliceStart = 0
	}
}

//=================================解析key=========================================
func parseKeyString(array *[]byte, inputFile string, param *Parameters) (string, int) {

	//首先读取一个字节，按照这个字节的前两个bit和后六个bit的值来判断
	//改如何读取后边的key值
	slice := readNextNByte(array, inputFile, 1, param)
	pre, suf := calByte(slice[0])
	str := getSliceValue(pre, suf, array, inputFile, param)

	return str, len(str)
}

//解析String Encoding value
//===========================String Encoding Value==============================
func parseStringEncodingValue(array *[]byte, inputFile string, param *Parameters) (string, int) {
	return parseKeyString(array, inputFile, param)
}

//解析Hashmap in Ziplist Encoding
func parseHashMapInZiplistEncodingValue(array *[]byte, inputFile string, param *Parameters) int {
	return parseZiplistEncodingValue(array, inputFile, param)
}

//=================================ZipList=======================================================
//解析Ziplist Encoding
func parseZiplistEncodingValue(array *[]byte, inputFile string, param *Parameters) int {
	return readContentByteAndParse(array, 10, inputFile, param)
}

func parseZipListContent(array *[]byte, param *Parameters) int {

	var length int
	//读取4个byte来获得zipList的长度
	var start uint64 = 8
	var end uint64 = 10
	listNum := changeByteToUint64(getArraySlice(start, end, array), binary.LittleEndian)

	start = end
	end = end + 1
	//首先读取1byte,这一byte代表前边的值的长度，
	// 如果这1byte的值小于254，那么这1byte的值就代表前边的值得长度，
	// 否则接下来的4个byte代表的值表示长度
	for i := listNum; i > 0; i-- {
		slice := getArraySlice(start, end, array)
		if slice[0] <= 253 {
			//这1bit的值就代表上一个值得长度
			//后边一byte就代表这次的值类型和长度
			//通过读取后边1byte的前缀n bits，获得值得类型和长度
			start = end
			end = end + 1
		} else {
			//If the first byte is 254, then the next 4 bytes are used to store the length.
			start = end
			end = end + 4
		}

		var startP *uint64 = &start
		var endP *uint64 = &end

		slice = getArraySlice(start, end, array)
		//读取这个slice代表的值的前2个或者4个bit
		num := parseZipListContentValueType(slice[0])
		str := parseZipListContentValue(startP, endP, num, array, slice[0])
		length = length + len(str)
		start = end
		end = end + 1
	}
	(*param).SliceStart = (*param).SliceStart + start

	return length
}

func parseZipListContentValueType(byte byte) (num int) {

	var resultValue int
	value := byte & 0xc0
	if uint8(value) == 0 { //00
		resultValue = 0
	} else if uint8(value) == 64 { //01
		resultValue = 1
	} else if uint8(value) == 128 { //10
		resultValue = 2
	} else {
		value := byte & 0xF0
		if uint8(value) == 192 { //1100
			resultValue = 3
		} else if uint8(value) == 208 { //1101
			resultValue = 4
		} else if uint8(value) == 224 { //1110
			resultValue = 5
		} else if uint8(value) == 240 {
			value := byte & 0x0f
			if uint8(value) == 0 { //11110000
				resultValue = 6
			} else if uint8(value) == 14 { //11111110
				resultValue = 7
			} else if uint8(value) >= 1 && uint8(value) <= 13 { //1111XXXX
				resultValue = 8
			}
		}
	}
	return resultValue
}

func parseZipListContentValue(start *uint64, end *uint64, num int, array *[]byte, byValue byte) string {
	var returnValue string

	switch num {

	case 0: // String value with length less than or equal to 63 bytes (6 bits).
		//证明前两个bit是00，后六个bit代表的值表示后边多少个bytes是值
		//读取后6个bits的值
		value := byValue & 0x3F
		*start = *end
		*end = *end + uint64(value)
		slice := getArraySlice(*start, *end, array)

		returnValue = assembelString(slice)

	case 1: //|01pppppp|qqqqqqqq| – 2 bytes : String value with length less than or equal to 16383 bytes (14 bits)
		//首先截取两个bytes
		var slice_value int16
		*end = *end + 1
		slice := getArraySlice(*start, *end, array)
		//把slice里边的两个字节转换成数字
		slice_buf := bytes.NewBuffer(slice)
		binary.Read(slice_buf, binary.BigEndian, &slice_value)
		value := slice_value & 0x3FFF
		*start = *end
		*end = *end + uint64(value)
		slice_content := getArraySlice(*start, *end, array)
		returnValue = assembelString(slice_content)
	case 2: //|10______|qqqqqqqq|rrrrrrrr|ssssssss|tttttttt| – 5 bytes : String value with length greater than or equal to 16384 bytes
		//首先截取5个bytes
		var slice_value uint64
		*end = *end + 4
		slice := getArraySlice(*start, *end, array)

		//把slice里边的5个字节转换成数字
		var temp [8]byte = [8]byte{}

		for index, _ := range temp {
			if index <= 2 {
				temp[index] = 0x00
			} else {
				temp[index] = slice[index-3]
			}
		}
		num := changeByteToUint64(temp[:], binary.BigEndian)
		slice_value = num & 0x0000003FFFFFFFFF

		*start = *end
		*end = *end + slice_value

		slice_content := getArraySlice(*start, *end, array)

		returnValue = assembelString(slice_content)
	case 3: // Read next 2 bytes as a 16 bit signed integer
		var x int16
		*start = *end
		*end = *end + 2
		slice := getArraySlice(*start, *end, array)
		slice_buf := bytes.NewBuffer(slice)
		binary.Read(slice_buf, binary.LittleEndian, &x)
		returnValue = strconv.Itoa(int(x))
	case 4: //Read next 4 bytes as a 32 bit signed integer
		var x int32
		*start = *end
		*end = *end + 4
		slice := getArraySlice(*start, *end, array)
		slice_buf := bytes.NewBuffer(slice)
		binary.Read(slice_buf, binary.LittleEndian, &x)
		returnValue = strconv.FormatInt(int64(x), 10)

	case 5: //Read next 8 bytes as a 64 bit signed integer
		var x int64
		*start = *end
		*end = *end + 8
		slice := getArraySlice(*start, *end, array)
		slice_buf := bytes.NewBuffer(slice)
		binary.Read(slice_buf, binary.LittleEndian, &x)
		returnValue = strconv.FormatInt(int64(x), 10)
	case 6: //Read next 3 bytes as a 24 bit signed integer
		*start = *end
		*end = *end + 3
		slice := getArraySlice(*start, *end, array)
		//根据最高位判断是补0x00还是0xFF
		cont := slice[2]
		value := cont & 0x80

		var temp [4]byte = [4]byte{}

		if value == 0 {
			for index, cont := range slice {
				temp[index] = cont
			}
			temp[3] = 0x00
		} else {
			for index, cont := range slice {
				temp[index] = cont
			}
			temp[3] = 0xFF
		}
		returnValue = strconv.FormatInt(changeByteToInt64(temp[:], binary.LittleEndian), 10)
	case 7: //Read next byte as an 8 bit signed integer
		*start = *end
		*end = *end + 1
		slice := getArraySlice(*start, *end, array)
		returnValue = strconv.Itoa(int(slice[0]))
	case 8:
		value := byValue & 0x0f
		returnValue = strconv.Itoa(int(value) - 1)
	}
	return returnValue
}

//==========================List Encoding==================================================
func parseListEncodingValue(array *[]byte, inputFile string, param *Parameters) int {
	return parseSetEncodingValue(array, inputFile, param)
}

//==========================Set Encoding===================================================
//解析Set Encoding
func parseSetEncodingValue(array *[]byte, inputFile string, param *Parameters) int {

	var length int
	//首先读取一个字节，按照这个字节的前两个bit和后六个bit的值来读取set的长度
	var setNum uint64 = readOneByteAndGetNum(array, inputFile, param)

	reIndex(array, inputFile, 1, param)

	for i := setNum; i > 0; i-- {
		slice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array)
		pre, suf := calByte(slice[0])
		str := getSliceValue(pre, suf, array, inputFile, param)
		length = length + len(str)
		reIndex(array, inputFile, 1, param)
	}

	(*param).SliceStart = (*param).SliceStart - 1
	(*param).SliceEnd = (*param).SliceEnd - 1
	return length
}

/**
直接读取一个slice的值，这个slice的值可能是string也可能是数字
*/
func getSliceValue(pre uint8, suf uint8, array *[]byte, inputFile string, param *Parameters) string {
	var returnValue string
	switch pre {
	case 0: //then the next 6 bits represent the length
		reIndex(array, inputFile, uint64(suf), param)
		returnValue = assembelString(getArraySlice((*param).SliceStart, (*param).SliceEnd, array))
	case 64: //then an additional byte is read from the stream. The combined 14 bits represent the length
		reIndexSliceEnd(array, inputFile, 1, param)
		slice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array)
		//把slice里边的两个字节转换成数字
		slice_value := changeByteToUint64(slice, binary.BigEndian)
		value := slice_value & 0x3FFF
		reIndex(array, inputFile, uint64(value), param)
		returnValue = assembelString(getArraySlice((*param).SliceStart, (*param).SliceEnd, array))
	case 128: //hen the remaining 6 bits are discarded. Additional 4 bytes are read from the stream, and those 4 bytes represent the length
		// (in big endian format in RDB version 6)
		var slice_value uint64
		reIndex(array, inputFile, 4, param)
		slice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array)
		//把slice里边的四个字节转换成数字
		slice_value = changeByteToUint64(slice, binary.BigEndian)
		reIndex(array, inputFile, slice_value, param)
		returnValue = assembelString(getArraySlice((*param).SliceStart, (*param).SliceEnd, array))
	case 192: //then the next object is encoded in a special format. The remaining 6 bits indicate the format
		switch suf {
		case 0: //0 indicates that an 8 bit integer follows.
			reIndex(array, inputFile, 1, param)
			returnValue = strconv.FormatInt(changeByteToInt64(getArraySlice((*param).SliceStart, (*param).SliceEnd, array), binary.LittleEndian), 10)
		case 1: //1 indicates that a 16 bit integer follows.
			reIndex(array, inputFile, 2, param)
			returnValue = strconv.FormatInt(changeByteToInt64(getArraySlice((*param).SliceStart, (*param).SliceEnd, array), binary.LittleEndian), 10)
		case 2: //2 indicates that a 32 bit integer follows.
			reIndex(array, inputFile, 4, param)
			returnValue = strconv.FormatInt(changeByteToInt64(getArraySlice((*param).SliceStart, (*param).SliceEnd, array), binary.LittleEndian), 10)
		case 3, 4: //实验证明C3代表压缩的字符串，不是C4
			//读取压缩字符串的长度
			compressed_length := readOneByteAndGetNum(array, inputFile, param)
			//读取未压缩字符串的长度
			uncompressed_length := readOneByteAndGetNum(array, inputFile, param)
			//解压缩字符串
			reIndex(array, inputFile, compressed_length, param)
			in_data := (*array)[(*param).SliceStart:(*param).SliceEnd]
			uncompressedByte := lzf.Lzf_dcompress(in_data, uncompressed_length)
			str := string(uncompressedByte)
			returnValue = str
		}
	}
	return returnValue
}

//=============================Intset Encoding=============================
//解析Intset类型
func parseIntSetEncoding(array *[]byte, inputFile string, param *Parameters) int {
	return readContentByteAndParse(array, 11, inputFile, param)
}

//解析IntSet
func parseIntSetContent(array *[]byte) int {

	var length int
	//首先读取四个字节，这四个字节是Encoding，
	//encoding的值为2，4或者8，表示每一个int类型
	//的数据存储在几个字节当中
	var start uint64 = 0
	var end uint64 = 0
	end = start + 4
	value := changeByteToInt64(getArraySlice(start, end, array), binary.LittleEndian)

	//在读取四个字节，这四个字节表示的是IntSet中元素的个数
	start = end
	end = end + 4
	setNum := changeByteToInt64(getArraySlice(start, end, array), binary.LittleEndian)

	//循环解析IntSet当中的元素

	for i := setNum; i > 0; i-- {
		start = end
		end = end + uint64(value)
		slice := getArraySlice(start, end, array)
		num := changeByteToInt64(slice, binary.LittleEndian)
		length = length + len(strconv.FormatInt(int64(num), 10))
	}
	return length
}

/**
读取代表内容的字节，并根据值类型解析
此方法只被：ZipMap Encoding, ZipList Encoding, parseIntSetEncoding调用，因为
他们在压缩的时候是所有值联合压缩
*/
func readContentByteAndParse(array *[]byte, types int, inputFile string, param *Parameters) int {

	var length int
	var pContentSlice *[]byte
	//首先读取一个字节，按照这个字节的前两个bit和后六个bit的值来判断
	//改如何读取后边的Content内容
	slice := readNextNByte(array, inputFile, 1, param)
	pre, suf := calByte(slice[0])

	switch pre {
	case 0: //then the next 6 bits represent the length
		contentSlice := readNextNByte(array, inputFile, uint64(suf), param)
		pContentSlice = &contentSlice
		length = callParseFunc(pContentSlice, types, param)
	case 64: //then an additional byte is read from the stream. The combined 14 bits represent the length

		reIndexSliceEnd(array, inputFile, 1, param)
		slice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array)
		//把slice里边的两个字节转换成数字
		slice_value := changeByteToUint64(slice, binary.BigEndian)
		//获取后14个bit代表的数字
		value := slice_value & 0x3FFF
		contentSlice := readNextNByte(array, inputFile, uint64(value), param)
		pContentSlice = &contentSlice
		length = callParseFunc(pContentSlice, types, param)
	case 128: //hen the remaining 6 bits are discarded. Additional 4 bytes are read from the stream, and those 4 bytes represent the length
		//(in big endian format in RDB version 6)
		var slice_value uint64

		//读取后4个bytes
		reIndex(array, inputFile, 4, param)
		slice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array)

		//把slice里边的四个字节转换成数字
		slice_value = changeByteToUint64(slice, binary.BigEndian)

		//读取这4bytes代表的数字的字节
		contentSlice := readNextNByte(array, inputFile, uint64(slice_value), param)
		pContentSlice = &contentSlice
		length = callParseFunc(pContentSlice, types, param)
	case 192: //then the next object is encoded in a special format. The remaining 6 bits indicate the format
		//在读取代表内容的字节里边，这里应该用不到,除了压缩
		switch suf {
		case 0: //0 indicates that an 8 bit integer follows.
			reIndex(array, inputFile, 1, param)
			slice_value := changeByteToInt64(getArraySlice((*param).SliceStart, (*param).SliceEnd, array), binary.LittleEndian)
			contentSlice := readNextNByte(array, inputFile, uint64(slice_value), param)
			pContentSlice = &contentSlice
			length = callParseFunc(pContentSlice, types, param)
		case 1: //1 indicates that a 16 bit integer follows.
			reIndex(array, inputFile, 2, param)
			slice_value := changeByteToInt64(getArraySlice((*param).SliceStart, (*param).SliceEnd, array), binary.LittleEndian)
			contentSlice := readNextNByte(array, inputFile, uint64(slice_value), param)
			pContentSlice = &contentSlice
			length = callParseFunc(pContentSlice, types, param)
		case 2: //2 indicates that a 32 bit integer follows.
			reIndex(array, inputFile, 4, param)
			slice_value := changeByteToInt64(getArraySlice((*param).SliceStart, (*param).SliceEnd, array), binary.LittleEndian)
			contentSlice := readNextNByte(array, inputFile, uint64(slice_value), param)
			pContentSlice = &contentSlice
			length = callParseFunc(pContentSlice, types, param)
		case 3, 4: //4 indicates that a compressed string follows.
			//读取压缩字符串的长度
			compressed_length := readOneByteAndGetNum(array, inputFile, param)
			//读取未压缩字符串的长度
			uncompressed_length := readOneByteAndGetNum(array, inputFile, param)
			//解压缩字符串
			reIndex(array, inputFile, compressed_length, param)
			in_data := (*array)[(*param).SliceStart:(*param).SliceEnd]
			unComByte := lzf.Lzf_dcompress(in_data, uncompressed_length)
			var pUnComByte *[]byte = &unComByte
			length = callParseFunc(pUnComByte, types, param)
		}
	}
	return length
}

//根据值类型调用相应的解析方法
func callParseFunc(content *[]byte, types int, param *Parameters) int {

	var length int
	switch types {
	case 10:
		length = parseZipListContent(content, param)
	case 9:
		length = parseZipMapContent(content)
	case 11:
		length = parseIntSetContent(content)
	}
	return length
}

//=========================ZipMap Encoding============================================
//array是ZipMap的内容
func parseZipMapContent(array *[]byte) int {
	var length int
	var start uint64 = 0
	var end uint64 = 0

	//略过一个字节，这个字节代表zipmap的长度(如果这一个字节表示不了长度，长度就无法获取)
	start = end
	end = end + 1

	//解析这一个字节，获得zipmap key value对数量
	slice := getArraySlice(start, end, array)
	var keyValueNum uint64
	if slice[0] >= 0 && slice[0] <= 252 { //slice[0]的值就代表key value对的数量
		keyValueNum = changeByteToUint64(slice, binary.BigEndian)
	} else if slice[0] == 253 { //读取后边的4bytes，这4bytes代表zipmap中key value对的数量
		start = end
		end = end + 4
		zipMapNum := getArraySlice(start, end, array)
		keyValueNum = changeByteToUint64(zipMapNum, binary.BigEndian)
	}

	//循环读取 key value pairs
	for i := 0; uint64(i) < keyValueNum; i++ {

		start = end
		end = end + 1

		slice := getArraySlice(start, end, array)

		//读取key的长度
		var num uint64
		if slice[0] >= 0 && slice[0] <= 252 { //slice[0]的值就代表key或者value的字节数
			num = changeByteToUint64(slice, binary.BigEndian)
		} else if slice[0] == 253 { //读取后边的4bytes，这4bytes代表zipmap中key或者value的字节数
			start = end
			end = end + 4
			num = changeByteToUint64(getArraySlice(start, end, array), binary.BigEndian)
		}

		//读取key
		start = end
		end = end + num
		keyStr := assembelString(getArraySlice(start, end, array))
		length = length + len(keyStr)
		//读取value的长度
		start = end
		end = end + 1
		valueLength := getArraySlice(start, end, array)
		var valueLengthNum uint64
		if valueLength[0] >= 0 && valueLength[0] <= 252 { //slice[0]的值就代表key或者value的字节数
			valueLengthNum = changeByteToUint64(valueLength, binary.BigEndian)
		} else if valueLength[0] == 253 { //读取后边的4bytes，这4bytes代表zipmap中key或者value的字节数
			start = end
			end = end + 4
			valueLengthNum = changeByteToUint64(getArraySlice(start, end, array), binary.BigEndian)
		}
		//读取并略过freeByte
		//读取freebyte
		start = end
		end = end + 1
		freeByte := getArraySlice(start, end, array)
		//略过freebyte
		start = start + changeByteToUint64(freeByte, binary.BigEndian)
		end = end + changeByteToUint64(freeByte, binary.BigEndian)
		//读取value
		start = end
		end = end + valueLengthNum
		valueStr := assembelString(getArraySlice(start, end, array))
		length = length + len(valueStr)

	}
	return length
}

func parseZipMapEncodingValue(array *[]byte, inputFile string, param *Parameters) int {
	return readContentByteAndParse(array, 9, inputFile, param)
}

/**
读取一个字节，按照这个字节获得表示的数字
此方法被: parseHashEncodingValue
		 parseSortedSetEncondingValue
		 parseSetEncodingValue
		 三个方法调用，用来获得value的数量
或者在解压缩时使用，用来获得压缩或者解压缩之后的字节数
*/
func readOneByteAndGetNum(array *[]byte, inputFile string, param *Parameters) uint64 {
	slice := readNextNByte(array, inputFile, 1, param)
	pre, suf := calByte(slice[0])
	var num uint64

	switch pre {
	case 0: //then the next 6 bits represent the length
		num = uint64(suf)
	case 64: //then an additional byte is read from the stream. The combined 14 bits represent the length
		reIndexSliceEnd(array, inputFile, 1, param)
		slice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array)
		//把slice里边的两个字节转换成数字
		slice_value := changeByteToUint64(slice, binary.BigEndian)
		//获取后14个bit代表的数字
		num = slice_value & 0x3FFF

	case 128: //hen the remaining 6 bits are discarded. Additional 4 bytes are read from the stream, and those 4 bytes represent the length
		//读取后4个bytes
		reIndex(array, inputFile, 4, param)
		slice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array)
		//把slice里边的四个字节转换成数字
		num = changeByteToUint64(slice, binary.BigEndian)

	case 192: //then the next object is encoded in a special format. The remaining 6 bits indicate the format
		switch suf {
		case 0: //0 indicates that an 8 bit integer follows.
			reIndex(array, inputFile, 1, param)
			num = changeByteToUint64(getArraySlice((*param).SliceStart, (*param).SliceEnd, array), binary.LittleEndian)
		case 1: //1 indicates that a 16 bit integer follows.
			reIndex(array, inputFile, 2, param)
			num = changeByteToUint64(getArraySlice((*param).SliceStart, (*param).SliceEnd, array), binary.LittleEndian)
		case 2: //2 indicates that a 32 bit integer follows.
			reIndex(array, inputFile, 4, param)
			num = changeByteToUint64(getArraySlice((*param).SliceStart, (*param).SliceEnd, array), binary.LittleEndian)
		case 3, 4: //4 indicates that a compressed string follows.
			logs.Log("error has occurred if you read this sentence. readOneByteAndGetNum")
		}
	}
	return num
}

//===================解析Hash Encoding==================================
func parseHashEncodingValue(array *[]byte, inputFile string, param *Parameters) int {

	var length int
	keyValueNum := readOneByteAndGetNum(array, inputFile, param)
	//循环读取key value
	var i uint64 = 0
	for ; i < keyValueNum*2; i++ {
		//计算偏移量并获得数据
		byteSlice := readNextNByte(array, inputFile, 1, param)
		pre, suf := calByte(byteSlice[0])
		str := getSliceValue(pre, suf, array, inputFile, param)

		length = length + len(str)
	}
	return length
}

//====================解析Sorted Set Encoding=======================
func parseSortedSetEncodingValue(array *[]byte, inputFile string, param *Parameters) int {

	var length int
	//读取hash map中key-value对的个数
	keyValueNum := readOneByteAndGetNum(array, inputFile, param)
	//循环读取key value
	var i uint64 = 0
	for ; i < keyValueNum*2; i++ {
		//计算偏移量并获得数据
		byteSlice := readNextNByte(array, inputFile, 1, param)
		pre, suf := calByte(byteSlice[0])
		str := getSliceValue(pre, suf, array, inputFile, param)
		length = length + len(str)
	}

	return length
}

/**
给定一个byte和一个位置，返回这个位置的前半部分(包括此位置)和后半部分
此方法用来获得一个byte的二进制表示的前半部分和后半部分
目前这个方法没有使用
*/
func binaryPrint(position int, byte byte) (front, back string) {
	str := strconv.FormatInt(int64(byte), 2)
	var value string = ""
	for i := 0; i < 8-len(str); i++ {
		value = value + "0"
	}
	value = value + str
	return string(value[:position]), string(value[position:])
}

//把一个byte数组按照16进制输出
func print(array []byte, length int) {
	var i int = 0
	var j int = 1
	//	for i = 0; i < len(array); i++ {
	for i = 0; i < length; i++ {

		if i == 0 {
			fmt.Print(j)
			j = j + 1
			fmt.Print("\t\t")
		}

		if i != 0 && i%16 == 0 {
			fmt.Println()
			fmt.Print(j)
			j = j + 1
			fmt.Print("\t\t")
		}
		fmt.Printf("%02X\t", array[i]&0xFF)
	}
	fmt.Println()
}

type ByteOrder interface {
	Uint16([]byte) uint16
	Uint32([]byte) uint32
	Uint64([]byte) uint64
	PutUint16([]byte, uint16)
	PutUint32([]byte, uint32)
	PutUint64([]byte, uint64)
	String() string
}

//把一个byte数组表示的数字转化成uint64
func changeByteToUint64(slice []byte, order binary.ByteOrder) uint64 {
	var returnValue uint64
	switch len(slice) {
	case 1: //一个byte
		var slice_value uint8
		slice_buf := bytes.NewBuffer(slice)
		binary.Read(slice_buf, order, &slice_value)
		returnValue = uint64(slice_value)
	case 2: //两个byte
		var slice_value uint16
		slice_buf := bytes.NewBuffer(slice)

		binary.Read(slice_buf, order, &slice_value)
		returnValue = uint64(slice_value)
	case 4: //四个byte
		var slice_value uint32
		slice_buf := bytes.NewBuffer(slice)
		binary.Read(slice_buf, order, &slice_value)
		returnValue = uint64(slice_value)
	default:
		var slice_value uint64
		slice_buf := bytes.NewBuffer(slice)
		binary.Read(slice_buf, order, &slice_value)
		returnValue = uint64(slice_value)
	}

	return returnValue
}

/**
把一个byte数组表示的数字转化成Int64
*/
func changeByteToInt64(slice []byte, order binary.ByteOrder) int64 {

	var returnValue int64
	switch len(slice) {
	case 1: //一个byte
		var slice_value int8
		slice_buf := bytes.NewBuffer(slice)
		binary.Read(slice_buf, order, &slice_value)
		returnValue = int64(slice_value)
	case 2: //两个byte
		var slice_value int16
		slice_buf := bytes.NewBuffer(slice)

		binary.Read(slice_buf, order, &slice_value)
		returnValue = int64(slice_value)
	case 4: //四个byte
		var slice_value int32
		slice_buf := bytes.NewBuffer(slice)
		binary.Read(slice_buf, order, &slice_value)
		returnValue = int64(slice_value)
	default:
		var slice_value int64
		slice_buf := bytes.NewBuffer(slice)
		binary.Read(slice_buf, order, &slice_value)
		returnValue = int64(slice_value)
	}

	return returnValue

}

/**
解析redis 数据库编号
*/
func parseRedisDBNum(array *[]byte, inputFile string, param *Parameters) uint64 {
	slice := readNextNByte(array, inputFile, 1, param)
	value := changeByteToUint64(slice, binary.BigEndian)
	return value
}

/**
解析redis rdb文件标志，redis rdb文件版本号等固定信息
*/
func parseFixedInformation(array *[]byte, param *Parameters) {
	//		var str string
	for i := 0; i < len((*param).Step); i++ {

		switch i {
		case 0:
			(*param).SliceEnd = (*param).SliceEnd + (*param).Step[0]
			slice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array) //0 5
			assembelString(slice)
		case 1:
			(*param).SliceStart = (*param).SliceStart + (*param).SliceEnd
			(*param).SliceEnd = (*param).SliceEnd + (*param).Step[1]
			slice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array) // 5 9
			assembelString(slice)
		}
	}
}

//===========================解析有效期==============================================
/**
此方法用来解析有效期(秒)
*/
func parseExpireTimeInSeconds(array *[]byte, inputFile string, param *Parameters) string {

	reIndex(array, inputFile, 4, param) //4 byte unsigned int
	timeSlice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array)
	timeString := changeByteToUint64(timeSlice, binary.LittleEndian)
	t := time.Unix(int64(timeString), int64(0))
	str := "expire time: " + t.Format("2006-01-02 15:04:05")
	return str
}

/**
此方法用来解析有效期(毫秒)
1秒 = 1000毫秒
1毫秒 = 1000微妙
1微妙 = 1000纳秒
1毫秒 = 1000000纳秒
*/
func parseExpireTimeInMillSeconds(array *[]byte, inputFile string, param *Parameters) string {
	reIndex(array, inputFile, 8, param) //8 byte unsigned long
	timeSlice := getArraySlice((*param).SliceStart, (*param).SliceEnd, array)
	timeString := changeByteToUint64(timeSlice, binary.LittleEndian)
	t := time.Unix(int64(0), int64(timeString)*1000000)
	str := "expire time: " + t.Format("2006-01-02 15:04:05")
	return str
}
