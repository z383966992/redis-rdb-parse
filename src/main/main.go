package main

/**
@author: zhouliangliang5
*/
import (
	"analysis"
	"config"
	"container/list"
	"dbutil"
	"io"
	"io/ioutil"
	"logs"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"parse"
	"regexp"
	"runtime"
	"strconv"
	"syscall"
	"time"
	"utils"
)

const (
	ip_regu     = "^(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])\\.(\\d{1,2}|1\\d\\d|2[0-4]\\d|25[0-5])$"
	num_regu    = "^(0|\\+?[1-9][0-9]*)$"
	pri_regu    = "^[0,1,2,3]$"
	status_requ = "^[0,1,2,3,4,5]$"
)

//读取文件等待时间
var multiply int64 = 30

//读取配置参数
var cliLocation string
var rdbLocation string
var port string

//设置redis-cli和同步后rdb文件的位置
func init() {
	conf := config.SetConfig("important.properties")
	cliLocation = conf.GetValue("param", "cli_location")
	rdbLocation = conf.GetValue("param", "rdb_location")
	port = conf.GetValue("port", "port")
	multiply, _ = strconv.ParseInt(conf.GetValue("timeout", "multiply"), 10, 32)
}

//首先要add task
//输入四个参数 1. host 2. port 3. filterLength 4.priority
func addTask(w http.ResponseWriter, r *http.Request) {
	//获取task数据
	host := r.FormValue("host")
	port := r.FormValue("port")
	filterLength := r.FormValue("filterLength")
	priority := r.FormValue("priority")
	//过滤某些key
	key := r.FormValue("key")
	//验证数据的有效性
	//非空
	if host == "" || port == "" {
		io.WriteString(w, "Three paramters need.Host is an ip address,port is a num !")
		return
	}
	// 验证ip地址的有效性
	regIp := regexp.MustCompile(ip_regu)
	if regIp.MatchString(host) == false {
		io.WriteString(w, "Host must be a ip address!")
		return
	}

	//	验证端口的有效性
	reg := regexp.MustCompile(num_regu)
	if reg.MatchString(port) == false {
		io.WriteString(w, "Port must be a numeric type!")
		return
	}
	//验证过滤长度的有效性

	if filterLength == "" {
		filterLength = "0"
	} else {
		regL := regexp.MustCompile(num_regu)
		if regL.MatchString(filterLength) == false {
			io.WriteString(w, "FilterLength must be a numeric type!")
			return
		}
	}

	//验证优先级有效性
	var prio int = 0
	if priority == "" {
		prio = 0
	} else {
		pri := regexp.MustCompile(pri_regu)
		if pri.MatchString(priority) == false {
			io.WriteString(w, "Priority must be 0, 1, 2 or 3!")
			return
		} else {
			value, err := strconv.ParseInt(priority, 10, 32)
			if err != nil {
				io.WriteString(w, "Priority error!")
				return
			} else {
				prio = int(value)
			}
		}
	}

	//向task数据库中添加task
	length, _ := strconv.ParseInt(filterLength, 10, 32)
	if length > 500 {
		io.WriteString(w, "filter length can not greater than 500!")
		return
	}
	taskId := analysis.GetCertificate()
	flag := dbutil.InsertTask(host, port, taskId, int(length), prio, key)

	//插入成功
	if flag {
		io.WriteString(w, "Job submit success,use "+taskId+" to check out result after several minutes. The waiting time dependent on the size of the rdb file and jobs before yours.")
	} else {
		//插入没成功
		io.WriteString(w, "Error occure while job submit, please try again later!")
	}
}

//为了部署多个实例的需要，修改了startParse
func startParse() {

	//每次只取一个任务，这样保证当存在多个服务时，每个机器都能获得任务
	logs.Log("starting parse!")
	for {
		mark, task := dbutil.FetchNewTask()

		if mark == 1 {
			if task.TaskId == "" {
				time.Sleep(time.Second * 10)
				continue
			} else {
				parseTask(task.Host, task.Port, task.FilterLength, task.FilterKey, task.TaskId)
			}
		} else if mark == -5 {
			logs.Log("task " + task.TaskId + " processing by another machine! ")
		} else {
			logs.Log("fetch task error!")
		}
	}
}

//解析程序
func parseTask(host string, port string, filterLength int, filterKey string, taskId string) {

	//获得机器的ip地址
	ip := utils.GetIpAddress()
	if ip == "unknown" {
		ip = utils.GetIpAddress2()
	}
	logs.Log("machine : " + ip + " processing task : " + taskId)
	//获取rdb文件
	rdbFile := rdbLocation + taskId + ".rdb"

	//如果文件存在则删除
	isExist := exists(rdbFile)
	if isExist == true {
		err := os.Remove(rdbFile)
		if err != nil {
			logs.Log("parseTask remove rdbFile error : " + err.Error())
			return
		}
	}

	flag := syncRdb(host, port, cliLocation, rdbFile, taskId)
	if flag == 1 {
		//还要判断解析是否成功了
		status := start(rdbFile, taskId, filterLength, filterKey)
		if status == true {
			dbutil.UpdateTaskStatus(1, taskId)
		} else {
			dbutil.UpdateTaskStatus(3, taskId)
		}
		//删除文件
		dropFile(rdbFile, taskId)
	} else {
		switch flag {
		case 2: //同步进程失效，文件大小无变化
			logs.Log("task " + taskId + " sync process error, file size no change!")
			dbutil.UpdateTaskStatus(2, taskId)
		case 4: //获得redis密码错误
			dbutil.UpdateTaskStatus(4, taskId)
			logs.Log("task " + taskId + " get remote redis server login password error!")
		}
		//删除文件
		dropFile(rdbFile, taskId)
	}
}

/**
inputFile 输入文件的路径
taskId 提交分析人物后会返回taskId用来查询
filterLength 过滤值长度大于某个值的key
*/
func start(inputFile string, taskId string, filterLength int, filterKey string) bool {

	var param parse.Parameters
	param = parse.Start(inputFile, filterLength, filterKey)
	if param.Success == true {
		analysis.StoreValueTypeStat(param.ResultMap, taskId)
		//		analysis.StoreValueLengthFilter(param.ValueLengthFilterList, taskId)
		analysis.StoreSortingResult(param.Heap, taskId)
		analysis.StoreKeyFiltResult(param.KeyFilterMap, taskId)
		return true
	} else {
		return false
	}
}

//判断文件是否存在
func exists(fileName string) bool {
	_, err := os.Stat(fileName)
	return err == nil || os.IsExist(err)
}

//解析操作完成之后删除文件
func dropFile(file, taskId string) {
	//首先判断存在文件吗
	isExist := exists(file)
	if isExist {
		err := os.Remove(file)
		if err != nil {
			logs.Log("task " + taskId + " remove file error : " + err.Error() + " --- " + file)
		}
	}
}

//每过一段时间读取文件的大小，如果文件大小有变化，那么证明正在同步
func checkSize(file string, fileCheck chan bool, taskId string) {
	var size int64 = 0
	var times int = 0 //记录没有同步成功时循环的次数
	var sync int = 0  //记录同步成功时循环的次数
	var zero int = 0  //记录读到文件，但是文件长度为0的次数
	var flag bool = false
	for {
		time.Sleep(time.Second * time.Duration(multiply*45))
		logs.Log("sleep time : " + time.Now().Format("2006-01-02 15:04:05"))
		if times >= 3 {
			fileCheck <- false
			break
		}

		//如果文件不存在，那么继续一次循环
		isExist := exists(file)
		if !isExist {
			times = times + 1
			logs.Log("taskId " + taskId + " times : " + strconv.FormatInt(int64(times), 10))
			continue
		}

		fileInfo, err := os.Stat(file)
		if err != nil {
			fileCheck <- false
			break
		}

		fileSize := fileInfo.Size()
		if fileSize == 0 {
			if zero >= 3 {
				fileCheck <- false
				break
			}
			zero = zero + 1
			logs.Log("taskId " + taskId + " zero : " + strconv.FormatInt(int64(zero), 10))
			continue
		}

		if fileSize != 0 && fileSize > size {
			size = fileSize
			flag = true
			logs.Log("taskId " + taskId + " fileSize : " + strconv.FormatInt(fileSize, 10))
		}
		if flag != true {
			if fileSize != 0 && fileSize == size {
				sync = sync + 1
				logs.Log("taskId " + taskId + " fileSize : " + strconv.FormatInt(fileSize, 10))
				logs.Log("taskId " + taskId + " sync : " + strconv.FormatInt(int64(sync), 10))
			}
		}

		flag = false

		if sync >= 3 {
			fileCheck <- true
			break
		}
	}
}

func syncRdb(host string, port string, cliLocation string, rdbFile string, taskId string) int {

	//获取redis登陆密码
	password := dbutil.GetRedisLogPassword(host, port)
	if password == "error" {
		logs.Log("task " + taskId + " syncRdb error can not get redis password")
		return 4
	}

	cmd := exec.Command(cliLocation, "-h", host, "-p", port, "-a", password, "--rdb", rdbFile)

	//同步rdb文件
	cmdChan := make(chan bool, 1)
	go cmdRun(cmd, cmdChan, taskId)

	//检查文件大小变化
	fileChan := make(chan bool, 1)
	go checkSize(rdbFile, fileChan, taskId)

	select {
	case value := <-fileChan:
		if value == false {
			if cmd.Process.Pid != 0 {
				logs.Log("task " + taskId + " sync failed process pid : " + strconv.FormatInt(int64(cmd.Process.Pid), 10))
				cmd.Process.Kill()
			}
			logs.Log("fileChan : false")
			return 2
		} else {
			logs.Log("fileChan : true")
			return 1
		}
	case value := <-cmdChan:
		if value == false {
			logs.Log("cmdChan : false")
			return 2
		} else {
			logs.Log("cmdChan : true")
			return 1
		}
	}
}

func cmdRun(cmd *exec.Cmd, channel chan bool, taskId string) {
	io, err := cmd.StderrPipe()
	if err != nil {
		logs.Log("task " + taskId + " cmdRun StderrPipe() error : " + err.Error())
		channel <- false
		return
	}
	logs.Log("sync cmd.start taskId " + taskId + " " + time.Now().Format("2006-01-02 15:04:05"))
	if err := cmd.Start(); err != nil {
		logs.Log("task " + taskId + " cmdRun Start() error : " + err.Error())
		channel <- false
		return
	}

	message, _ := ioutil.ReadAll(io)

	if err := cmd.Wait(); err != nil {
		logs.Log("task " + taskId + " cmdRun sync rdb file error : " + string(message) + " : " + err.Error())
		channel <- false
		return
	}
	logs.Log("sync cmd.end taskId " + taskId + " " + time.Now().Format("2006-01-02 15:04:05"))
	channel <- true
}

//查看每一个value类型有多少个key
func viewValueTypeStatResult(w http.ResponseWriter, r *http.Request) {
	taskId := r.FormValue("taskId")

	if taskId == "" {
		io.WriteString(w, "Need taskId!")
		return
	}

	result := dbutil.FetchResult(string(taskId), "1", 1000, 0)

	var listHtml string = "<body><ol>"
	for _, cont := range result {
		listHtml += "<li>" + cont + "</li>"
	}

	listHtml = listHtml + "</ol></body>"
	io.WriteString(w, listHtml)
}

//查看value length 过滤结果
//默认显示100条
//page从地址栏中输入
func viewValueLengthFilterResult(w http.ResponseWriter, r *http.Request) {
	taskId := r.FormValue("taskId")
	page := r.FormValue("page")
	types := r.FormValue("type")

	//验证taskId是否输入
	if taskId == "" {
		io.WriteString(w, "Need taskId!")
		return
	}

	//验证page是否输入
	reg := regexp.MustCompile(num_regu)
	if reg.MatchString(page) == false {
		io.WriteString(w, "Need param page, page must be a numeric type!")
		return
	}

	pageNum, _ := strconv.ParseInt(page, 10, 32)
	var offset int = 0
	if pageNum == 0 || pageNum == 1 {
		offset = 0
	} else {
		offset = (int(pageNum) - 1) * 100
	}

	if types == "" {
		types = "2"
	}

	result := dbutil.FetchResult(string(taskId), types, 100, offset)

	var listHtml string = "<body>" +
		"<p>each page show 100 records</p>" + "<ol>"
	for _, cont := range result {
		listHtml += "<li>" + cont + "</li>"
	}

	listHtml = listHtml + "</ol></body>"
	io.WriteString(w, listHtml)
}

//查看某状态的任务
func taskView(w http.ResponseWriter, r *http.Request) {

	status := r.FormValue("status")

	reg := regexp.MustCompile(status_requ)
	if reg.MatchString(status) == false {
		io.WriteString(w, "need param status, status must be 0-new task ,1-success,2-can not get rdb,3-parse fatal, 4-get remote redis password error or 5-task is running!")
		return
	}

	int_stat, _ := strconv.ParseInt(status, 10, 32)

	mark, list := dbutil.FetchTask(int(int_stat))

	if mark == 1 {
		var listHtml string = "<body><ol>"
		for e := list.Front(); e != nil; e = e.Next() {
			task, ok := e.Value.(dbutil.Task)
			if ok {
				//获取task信息
				host := task.Host
				port := task.Port
				filterLength := task.FilterLength
				filterKey := task.FilterKey
				taskId := task.TaskId
				createTime := task.CreateTime
				listHtml += "<li>" + "host:" + host + ";port:" + port + ";filterLength:" + strconv.FormatInt(int64(filterLength), 10) + ";filterKey:" + filterKey + ";taskId:" + taskId + ";create_time:" + createTime + "</li>"
			} else {
				//task的类型不正确
				return
			}
		}
		listHtml = listHtml + "</ol></body>"
		io.WriteString(w, listHtml)
	} else {
		io.WriteString(w, "error occure !")
		logs.Log("error occure in taskView!")
	}
}

//根据taskId查看某一个task
func task(w http.ResponseWriter, r *http.Request) {

	taskId := r.FormValue("taskId")

	mark, list := dbutil.FetchTaskById(taskId)
	if list.Len() == 0 {
		io.WriteString(w, "without this task !")
		return
	}

	if mark == 1 {
		var listHtml string = "<body><ol>"
		for e := list.Front(); e != nil; e = e.Next() {
			task, ok := e.Value.(dbutil.Task)
			if ok {
				//获取task信息
				host := task.Host
				port := task.Port
				filterLength := task.FilterLength
				filterKey := task.FilterKey
				taskId := task.TaskId
				priority := task.Priority
				status := task.Status
				createTime := task.CreateTime
				listHtml += "<li>" + "host:" + host + ";port:" + port + ";filterLength:" + strconv.FormatInt(int64(filterLength), 10) + ";filter_key:" + filterKey + ";taskId:" + taskId +
					";priority:" + strconv.FormatInt(int64(priority), 10) + ";status:" + strconv.FormatInt(int64(status), 10) + ";create_time:" + createTime + "</li>"
			} else {
				//task的类型不正确
				return
			}
			listHtml = listHtml + "</ol></body>"
		}
		io.WriteString(w, listHtml)
	} else {
		io.WriteString(w, "error occure !")
		logs.Log("error occure in taskView!")
	}
}

//用于删除N天之前的任务
func delete(w http.ResponseWriter, r *http.Request) {

	//num必须是数字类型，会对应删除n天之前的任务
	num := r.FormValue("num")

	reg := regexp.MustCompile(num_regu)
	if reg.MatchString(num) == false {
		io.WriteString(w, "Need param num, num must be a numeric type!")
		return
	}

	n, _ := strconv.ParseInt(num, 10, 32)
	//得到n天前的零点
	unixPatt := timeComp(n)

	//获得所有的任务的task_id和create_time
	mark, result := dbutil.FetchTaskList()

	taskList := list.New()

	//遍历task，比较时间
	if mark == 1 {
		for e := result.Front(); e != nil; e = e.Next() {
			task, ok := e.Value.(dbutil.Task)
			if ok {
				//获取task信息
				taskId := task.TaskId
				createTime := task.CreateTime
				tt, _ := time.Parse("2006-01-02 15:04:05", createTime)
				taskUnix := tt.Unix()

				//task创建时间在unixPatt之前
				if taskUnix < unixPatt {
					taskList.PushBack(taskId)
				}
			} else {
				io.WriteString(w, "error occure, try again later! type error")
				return
			}
		}
		dbutil.DeleteTask(taskList)
		io.WriteString(w, "success!")
		return
	} else {
		io.WriteString(w, "error occure, try again later! mark != 1")
		return
	}
}

//n天前时间的unix()形式
func timeComp(num int64) int64 {
	d, _ := time.ParseDuration("-24h")
	date := time.Now().Add(d * time.Duration(num))
	year := date.Year()
	month := int(date.Month())
	day := date.Day()
	monthStr := ""
	dayStr := ""

	if month < 10 {
		monthStr = "0" + strconv.FormatInt(int64(month), 10)
	} else {
		monthStr = strconv.FormatInt(int64(month), 10)
	}

	if day < 10 {
		dayStr = "0" + strconv.FormatInt(int64(day), 10)
	} else {
		dayStr = strconv.FormatInt(int64(day), 10)
	}

	dateString := strconv.FormatInt(int64(year), 10) + "-" + monthStr + "-" + dayStr + " 00:00:00"
	da, _ := time.Parse("2006-01-02 15:04:05", dateString)
	unixPatt := da.Unix()
	return unixPatt
}

func main() {

	//	//本地运行
	//	inputFile := "C:/Users/Administrator/Downloads/6410.rdb"
	//	inputFile := "D:/software/work/redis-2.8.19/6383/dump.rdb"
	//	inputFile := "D:/software/1431429678905877955.rdb"
	//	filterLength := 100
	//	taskId := analysis.GetCertificate()
	//	fmt.Println(taskId)
	//	var param parse.Parameters
	//	param = parse.Start(inputFile, filterLength, "")
	//	if param.Success == true {
	//		fmt.Println(param.TotalNum)
	//		//		analysis.StoreValueTypeStat(param.ResultMap, taskId)
	//		//		analysis.StoreValueLengthFilter(param.ValueLengthFilterList, taskId)
	//		//		analysis.OutPut(param.Heap)
	//		analysis.StoreSortingResult(param.Heap, taskId)
	//		analysis.StoreKeyFiltResult(param.KeyFilterMap, taskId)
	//		fmt.Println("success")
	//	} else {
	//		fmt.Println("error")
	//	}
	////////////////////////////////////////////////////////////////////////////////
	defer func() {
		if r := recover(); r != nil {
			logs.Log(r)
		}
	}()
	exitChan := make(chan int)
	signalChan := make(chan os.Signal, 1)
	go func() {
		<-signalChan
		exitChan <- 1
	}()

	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	//设置程序使用的cup数量
	cpu := runtime.NumCPU()
	if cpu == 1 {
		cpu = 1
	} else {
		cpu = cpu / 2
	}
	runtime.GOMAXPROCS(cpu)
	go startParse()

	//添加任务并启动解析服务
	http.HandleFunc("/add_task", addTask)
	//查看某种状态的task
	http.HandleFunc("/task_view", taskView)
	//根据taskid查看task
	http.HandleFunc("/task", task)
	//查看value类型统计
	http.HandleFunc("/result/value/type", viewValueTypeStatResult)
	//查看value长度过滤
	http.HandleFunc("/result/value/length", viewValueLengthFilterResult)
	//删除任务
	http.HandleFunc("/delete", delete)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		logs.Log("error in main : " + err.Error())
		os.Exit(-1)
	}
}
