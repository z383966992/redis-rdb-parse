package dbutil

/**
@author: zhouliangliang5
*/
import (
	"config"
	"container/list"
	"database/sql"
	_ "github.com/Go-SQL-Driver/MySQL"
	"logs"
)

var db *sql.DB

func init() {
	conf := config.SetConfig("important.properties")
	connection := conf.GetValue("database", "connection")
	db, _ = sql.Open("mysql", connection)
	db.SetMaxOpenConns(200)
	db.SetMaxIdleConns(100)
	db.Ping()
}

type Task struct {
	Host         string
	Port         string
	FilterLength int
	FilterKey    string
	TaskId       string
	Priority     int
	Status       int
	CreateTime   string
	UpdateTime   string
}

//获取新的任务
func FetchNewTask() (int, Task) {

	var task Task

	//查询
	stmtQuery, err := db.Prepare("select host, port, filter_length, filter_key, task_id, create_time, update_time from rdb_parse_task where status = 0 order by priority desc limit 1")
	defer stmtQuery.Close()
	if err != nil {
		logs.Log("fetch new task error in dbutil db.stmtQuery Prepare : " + err.Error())
		return -1, task
	}

	rows, errs := stmtQuery.Query()

	if errs != nil {
		logs.Log("fetch new task error in dbutil stmtIns.Query Exec : " + errs.Error())
		return -2, task
	}

	var host string
	var port string
	var filterLength int
	var filterKey string
	var taskId string
	var createTime string
	var updateTime string
	for rows.Next() {
		rows.Scan(&host, &port, &filterLength, &filterKey, &taskId, &createTime, &updateTime)
		task.Host = host
		task.Port = port
		task.FilterLength = filterLength
		task.TaskId = taskId
		task.CreateTime = createTime
		task.UpdateTime = updateTime
		task.FilterKey = filterKey
	}
	if taskId != "" {
		//更改任务状态
		stmtUpdate, errUpdate := db.Prepare("update rdb_parse_task set status = 5 where task_id = ? and update_time = ?")
		defer stmtUpdate.Close()

		if errUpdate != nil {
			logs.Log("fetch new task error in dbutil db.stmtUpdate : " + errUpdate.Error())
			return -3, task
		}

		result, err := stmtUpdate.Exec(taskId, updateTime)
		if err != nil {
			logs.Log("fetch new task error in dbutil db.stmtUpdate Exec : " + err.Error())
			return -4, task
		}

		value, _ := result.RowsAffected()

		//证明记录没有被修改过
		if value == 1 {
			return 1, task
		} else {
			//证明记录已经被修改了，有其它的实例获得了任务
			return -5, task
		}
	} else {
		return 1, task
	}
}

//存储分析结果
func Store(content, types, taskId string) {

	stmtIns, err := db.Prepare("insert into rdb_anly_result(content, type, task_id, create_time) values (?,?,?,now())")

	if err != nil {
		logs.Log("store error in dbutil db.Prepare : " + err.Error())
		return
	}
	defer stmtIns.Close()

	_, errs := stmtIns.Exec(content, types, taskId)

	if errs != nil {
		logs.Log("store error in dbutil stmtIns.Exec : " + errs.Error())
		return
	}
}

//根据taskId和type获取分析结果
func FetchResult(taskId string, types string, limit int, offset int) []string {
	result := make([]string, 0)
	stmtIns, err := db.Prepare("select content from rdb_anly_result where task_id = ? and type = ? order by id desc limit ? offset ? ")

	if err != nil {
		logs.Log("fetch result error in dbutil db.Prepare!" + " : " + err.Error())
		return result
	}

	defer stmtIns.Close()

	rows, errs := stmtIns.Query(taskId, types, limit, offset)
	if errs != nil {
		logs.Log("fetch result error in dbutil stmtIns.Query!" + " : " + errs.Error())
		return result
	}

	var content string

	for rows.Next() {
		rows.Scan(&content)
		result = append(result, content)
	}
	return result
}

//向任务表中插入任务
func InsertTask(host, port, taskId string, filterLength, priority int, filter_key string) bool {

	stmtIns, err := db.Prepare("insert into rdb_parse_task (host, port, filter_length, filter_key, task_id, priority, status, create_time) values (?,?,?,?,?,?,0,now())")

	if err != nil {
		logs.Log("insert task error in dbutil db.Prepare : " + err.Error())
		return false
	}
	defer stmtIns.Close()

	_, errs := stmtIns.Exec(host, port, filterLength, filter_key, taskId, priority)
	if errs != nil {
		logs.Log("insert task error in dbutil stmtIns.Exec : " + errs.Error())
		return false
	}
	return true
}

//更新任务状态
func UpdateTaskStatus(status int, taskId string) bool {
	stmtIns, err := db.Prepare("update rdb_parse_task set status = ? where task_id = ?")

	if err != nil {
		logs.Log("update task status error in dbutil db.Prepare : " + err.Error())
		return false
	}
	defer stmtIns.Close()

	_, errs := stmtIns.Exec(status, taskId)
	if errs != nil {
		logs.Log("update task status error in dbutil stmtIns.Exec : " + errs.Error())
		return false
	}
	return true
}

//获取所有任务的任务列表
func FetchTaskList() (int, *list.List) {
	var stmtIns *sql.Stmt
	var err error
	stmtIns, err = db.Prepare("select task_id, create_time from rdb_parse_task")

	if err != nil {
		logs.Log("fetch task list error in dbutil db.Prepare : " + err.Error())
		return -1, list.New()
	}

	defer stmtIns.Close()

	rows, errs := stmtIns.Query()

	if errs != nil {
		logs.Log("fetch task list error in dbutil stmtIns.Query : " + errs.Error())
		return -2, list.New()
	}

	taskList := list.New()

	var taskId string
	var createTime string
	for rows.Next() {
		var task Task
		rows.Scan(&taskId, &createTime)
		task.TaskId = taskId
		task.CreateTime = createTime
		taskList.PushBack(task)
	}
	return 1, taskList
}

//根据任务状态获取任务列表
func FetchTask(status int) (int, *list.List) {

	var stmtIns *sql.Stmt
	var err error

	stmtIns, err = db.Prepare("select host, port, filter_length, filter_key, task_id, create_time from rdb_parse_task where status = ? order by create_time desc")

	if err != nil {
		logs.Log("fetch task error in dbutil db.Prepare : " + err.Error())
		return -1, list.New()
	}

	defer stmtIns.Close()

	rows, errs := stmtIns.Query(status)

	if errs != nil {
		logs.Log("fetch task error in dbutil stmtIns.Query : " + errs.Error())
		return -2, list.New()
	}

	taskList := list.New()

	var host string
	var port string
	var filterLength int
	var filterKey string
	var taskId string
	var createTime string
	for rows.Next() {
		var task Task
		rows.Scan(&host, &port, &filterLength, &filterKey, &taskId, &createTime)
		task.Host = host
		task.Port = port
		task.FilterLength = filterLength
		task.TaskId = taskId
		task.CreateTime = createTime
		task.FilterKey = filterKey
		taskList.PushBack(task)
	}
	return 1, taskList
}

//根据任务id获取单个任务
func FetchTaskById(task_id string) (int, *list.List) {
	stmtIns, err := db.Prepare("select host, port, filter_length, filter_key, task_id, priority, status, create_time from rdb_parse_task where task_id = ?")

	if err != nil {
		logs.Log("fetch task by id error in dbutil db.Prepare : " + err.Error())
		return -1, list.New()
	}

	defer stmtIns.Close()

	rows, errs := stmtIns.Query(task_id)

	if errs != nil {
		logs.Log("fetch task by id error in dbutil stmtIns.Query : " + errs.Error())
		return -2, list.New()
	}

	taskList := list.New()

	var host string
	var port string
	var filterLength int
	var filterKey string
	var taskId string
	var priority int
	var status int
	var createTime string
	for rows.Next() {
		var task Task
		rows.Scan(&host, &port, &filterLength, &filterKey, &taskId, &priority, &status, &createTime)
		task.Host = host
		task.Port = port
		task.FilterLength = filterLength
		task.TaskId = taskId
		task.Priority = priority
		task.Status = status
		task.CreateTime = createTime
		task.FilterKey = filterKey
		taskList.PushBack(task)
	}
	return 1, taskList
}

//根据ip地址和端口号获取redis登陆密码
func GetRedisLogPassword(host, port string) string {
	stmtIns, err := db.Prepare("select instance_passwd from instance where instance_ip = ? and instance_port = ?")

	if err != nil {
		logs.Log("get redis log password error in dbutil db.Prepare : " + err.Error())
		return "error"
	}

	defer stmtIns.Close()

	rows, errs := stmtIns.Query(host, port)

	if errs != nil {
		logs.Log("get redis log password error in dbutil stmtIns.Query : " + errs.Error())
		return "error"
	}

	var instancePassword string
	for rows.Next() {
		rows.Scan(&instancePassword)
	}
	return instancePassword
}

//删除任务，以及任务的分析结果
func DeleteTask(list *list.List) bool {

	stmtInsTask, err := db.Prepare("delete from rdb_parse_task where task_id = ?")

	if err != nil {
		logs.Log("delete task stmtInsTask error in dbutil : " + err.Error())
		return false
	}

	defer stmtInsTask.Close()

	stmtResult, err := db.Prepare("delete from rdb_anly_result where task_id = ?")
	if err != nil {
		logs.Log("delete dask stmtResult error in dbutil : " + err.Error())
		return false
	}

	defer stmtResult.Close()

	for e := list.Front(); e != nil; e = e.Next() {
		taskId, ok := e.Value.(string)
		if ok {
			stmtInsTask.Exec(taskId)
			stmtResult.Exec(taskId)
		} else {
			logs.Log("type error!")
		}
	}
	return true
}
