package lzf

/*
#include "lzfP.h"
#include "lzf_c.cpp"
#include "lzf_d.cpp"
#include <stdio.h>
#include <stdlib.h>
void output(char *str) {
printf("%s\n", str);
}
*/
import "C" //此处和上面的C代码一定不能有空行 否则会报错

import (
	"fmt"
	"unsafe"
)

//lzf压缩算法，调用c的函数
func Lzf_compress(str string) []byte {

	cstr := C.CString(str)
	out_data := make([]byte, len(str)*2)

	result := C.lzf_compress(unsafe.Pointer(cstr), C.uint(len(str)), unsafe.Pointer(&out_data[0]), C.uint(len(str)*2))
	C.free(unsafe.Pointer(cstr))

	return out_data[:result]
}

//lzf解压缩算法，调用c的函数
func Lzf_dcompress(in_data []byte, length uint64) []byte {
	out_data := make([]byte, length)
	C.lzf_decompress(unsafe.Pointer(&in_data[0]), C.uint(len(in_data)), unsafe.Pointer(&out_data[0]), C.uint(length))
	return out_data
}

func print(array []byte) {
	for i := 0; i < len(array); i++ {
		fmt.Printf("% X ", array[i])
	}
	fmt.Println()
}
