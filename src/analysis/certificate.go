package analysis

/**
@author: zhouliangliang5
*/
import (
	"strconv"
	"time"
)

//获得一个文件解析凭证

func GetCertificate() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
}
