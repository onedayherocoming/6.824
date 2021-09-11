package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskStatusReady int = iota
	TaskStatusQueue //进入等待队列
	TaskStatusRunning  //task运行状态
	TaskStatusFinish
	TaskStatusErr
)
const (
	MaxTaskRuntime = time.Second*5
	ScheduleInterval = time.Millisecond*5 
)

type TaskStat struct {
	Status int
	WorkerID int
	StartTime time.Time
}

//coordinator的结构体
type Coordinator struct {
	files []string
	nReduce int
	taskPhase TaskPhase
	taskStats []TaskStat
	mu sync.Mutex
	done bool
	workerSeq int  //work id 生成变量
	taskCh chan Task
}



//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce   to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mu = sync.Mutex{}
	c.nReduce = nReduce
	c.files = files
	if nReduce > len(files){
		c.taskCh = make(chan Task,nReduce)
	}else{
		c.taskCh = make(chan Task,len(c.files))
	}
	c.initMapTask()
	go c.tickSchedule()
	c.server()
	DPrintf("coordinator init")
	return &c
}

//TestRPC 测试
func (c *Coordinator) TestRPC(args *TestArgs,reply *TestReply) error{
	fmt.Println("成功")
	reply.Res = args.X + args.Y
	return nil;
}

// RegWorker 分配一个id(seq)给worker
func (c* Coordinator) RegWorker(args *RegisterArgs,reply *RegisterReply)error{
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workerSeq++
	reply.WorkerID = c.workerSeq
	return nil
}

//注册任务
func (c *Coordinator) regTask(args *TaskArgs,task *Task){
	c.mu.Lock()
	defer c.mu.Unlock()

	if task.Phase != c.taskPhase{
		panic("request task phase not match")
	}
	c.taskStats[task.Seq].Status = TaskStatusRunning
	c.taskStats[task.Seq].WorkerID = args.WorkerID
	c.taskStats[task.Seq].StartTime = time.Now()
}


//给worker分配一个任务
func (c *Coordinator) GetOneTask(args *TaskArgs,reply *TaskReply)error{
	if(c.Done()){//如果已经完成，则返回空
		reply.Task = nil
		return nil;
	}
	task:= <- c.taskCh
	reply.Task = &task
	if task.Alive{
		c.regTask(args,&task)
	}
	DPrintf("in get one task,args: %+v,reply: %+v",args,reply)
	return nil;
}

// 汇报task的结果
func (c *Coordinator) ReportTask(args *ReportTaskArgs,reply* ReportTaskArgs)error{
	c.mu.Lock()
	defer c.mu.Unlock()

	DPrintf("get task report: %+v, taskPhase: %+v",args,c.taskPhase)
	if c.taskPhase != args.Phase || args.WorkerID !=c.taskStats[args.Seq].WorkerID{
		return nil
	}
	if args.Done{
		c.taskStats[args.Seq].Status = TaskStatusFinish
	}else{
		c.taskStats[args.Seq].Status = TaskStatusErr
	}
	go c.schedule()
	return nil
}

//初始化map任务
func (c *Coordinator) initMapTask(){
	c.taskPhase = MapPhase
	c.taskStats = make([]TaskStat,len(c.files))
}

//在map任务都完成之后，初始化reduce任务
func (c *Coordinator) initReduceTask(){
	DPrintf("init ReduceTask")
	c.taskPhase = ReducePhase
	c.taskStats = make([]TaskStat,c.nReduce)
}

func (c *Coordinator) tickSchedule(){
	for !c.Done(){
		go c.schedule()
		time.Sleep(ScheduleInterval)
	}
}

func (c *Coordinator)schedule(){
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done{
		return
	}

	allFinish :=true
	for index,t := range c.taskStats{
		switch t.Status{
		case TaskStatusReady:
			allFinish = false
			c.taskCh <- c.getTask(index)
			c.taskStats[index].Status = TaskStatusQueue
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			if(time.Now().Sub(t.StartTime)>MaxTaskRuntime){
				c.taskStats[index].Status = TaskStatusQueue //运行超时，进入队列重新分配
				c.taskCh <- c.getTask(index)
			}
		case TaskStatusFinish:
		case TaskStatusErr:
			allFinish = false
			c.taskStats[index].Status = TaskStatusQueue //运行时出错，重新加入队列运行
		default:
			panic("t.status err")
		}
	}
	if allFinish == true{
		if c.taskPhase == MapPhase{
			c.initReduceTask()
		}else{
			c.done = true
		}
	}
}

func (c *Coordinator) getTask(taskSeq int) Task{
	task := Task{
		FileName :"",
		NReduce: c.nReduce,
		NMap: len(c.files),
		Seq: taskSeq,
		Phase : c.taskPhase,
		Alive: true,
	}
	DPrintf("c: %+v,taskSeq: %d, lenFiles: %d, lenTaskStatus: %d",c,taskSeq,len(c.files),len(c.taskStats))
	if task.Phase == MapPhase{
		task.FileName = c.files[taskSeq]
	} 
	return task
}
