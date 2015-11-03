package dbutil

//import (
//	"config"
//	_ "github.com/Go-SQL-Driver/MySQL"
//	"github.com/go-xorm/xorm"
//	"logs"
//	"strconv"
//)
//
//var engine *xorm.Engine
//
//func init() {
//	conf := config.SetConfig("important.properties")
//	connection := conf.GetValue("database", "connection")
//	engine, _ = xorm.NewEngine("mysql", connection)
//	engine.ShowSQL = false
//	engine.ShowDebug = false
//	engine.ShowWarn = true
//	engine.SetMaxIdleConns(5)
//	engine.SetMaxOpenConns(10)
//}

//获取任务(要解决多实例竞争问题)
//func FetchNewTask() (int, Task) {
//
//	var task Task
//
//	session := engine.NewSession()
//
//	defer session.Close()
//	defer session.DB().Exec("unlock tables")
//	_, errE := session.DB().Exec("lock table rdb_parse_task write")
//
//	if errE != nil {
//		logs.Log("fetch new task error in dbutil lock table : " + errE.Error())
//		return -1, task
//	}
//
//	result, err := session.Query("select host, port, filter_length, task_id, create_time from rdb_parse_task where status = 0 order by priority desc limit 1")
//
//	if err != nil {
//		logs.Log("fetch new task error in dbutil session.Query : " + err.Error())
//		return -2, task
//	}
//
//	if len(result) == 1 {
//		var host string = string(result[0]["host"])
//		var port string = string(result[0]["port"])
//		value, _ := strconv.ParseInt(string(result[0]["filter_length"]), 10, 32)
//		var filterLength int = int(value)
//		var taskId string = string(result[0]["task_id"])
//		var createTime string = string(result[0]["create_time"])
//
//		task.Host = host
//		task.Port = port
//		task.FilterLength = filterLength
//		task.TaskId = taskId
//		task.CreateTime = createTime
//
//		_, errs := session.Exec("update rdb_parse_task set status = 5 where task_id = ? ", taskId)
//
//		if errs != nil {
//			logs.Log("fetch new task error in dbutil sesssion.Exec update status : " + errs.Error())
//		}
//	}
//	return 1, task
//}
