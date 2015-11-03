package logs

/**
@author: zhouliangliang5
*/
import (
	"config"
	"fmt"
	"log"
	"os"
)

var logger *log.Logger

//judge file exists
func exists(fileName string) bool {
	_, err := os.Stat(fileName)
	return err == nil || os.IsExist(err)
}

func init() {

	conf := config.SetConfig("important.properties")
	logLocation := conf.GetValue("log", "log_location")

	logFile, err := os.OpenFile(logLocation, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0664)
	if err != nil {
		fmt.Println("in logger : " + err.Error())
		os.Exit(-1)
	}
	logger = log.New(logFile, "n", log.Ldate|log.Ltime|log.Llongfile)
}

//func Log(value string) {
//	logger.Println(value)
//
//}

func Log(value interface{}) {
	logger.Println(value)
}
