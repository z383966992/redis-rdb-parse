package utils

//获得本机ip地址
import (
	"logs"
	"net"
	"strings"
)

/**
net 环境下能够获得访问网络的ip
*/
func GetIpAddress() string {
	conn, err := net.Dial("udp", "www.baidu.com:80")
	if err != nil {
		logs.Log(err.Error())
		return "unknown"
	}
	defer conn.Close()
	return strings.Split(conn.LocalAddr().String(), ":")[0]
}

/**
能够得到所有的ip
*/
func GetIpAddress2() string {
	var ip string
	inter, err := net.InterfaceAddrs()

	if err != nil {
		logs.Log(err.Error())
		return "unknown"
	}

	for _, in := range inter {
		ip += in.String() + " "
	}
	return ip
}

/**
能够得到mac地址
*/
//func getMACAddress() string {
//	interfaces, err := net.Interfaces()
//	if err != nil {
//		fmt.Println(err.Error())
//		logs.Log(err.Error())
//		return ""
//	}
//
//	for _, inter := range interfaces {
//		mac := inter.HardwareAddr
//		fmt.Println("MAC=", mac)
//	}
//	return ""
//}
