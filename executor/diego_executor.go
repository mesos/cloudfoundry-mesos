package executor

import (
	"fmt"
	"time"
	"sync"
	"encoding/json"

	"github.com/gogo/protobuf/proto"

	exec "github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"

	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/pivotal-golang/localip"
	"github.com/cloudfoundry-incubator/rep"
)

type DiegoExecutor struct {
	cellClient rep.Client
	taskStateMap map[string]mesos.TaskState
	lock sync.RWMutex
}

func NewDiegoExecutor() *DiegoExecutor {
	ip, _ := localip.LocalIP()
	address := fmt.Sprintf("http://%s:%v", ip, 1800)

	repClientFactory := rep.NewClientFactory(cf_http.NewClient())
	cellClient := repClientFactory.CreateClient(address)
	return &DiegoExecutor{
		cellClient: cellClient,
		taskStateMap: map[string]mesos.TaskState{},
	}
}

func (e *DiegoExecutor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Registered Executor on slave ", slaveInfo.GetHostname())
	go e.watchTasks(driver)
}

func (e *DiegoExecutor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Re-registered Executor on slave ", slaveInfo.GetHostname())
}

func (e *DiegoExecutor) Disconnected(exec.ExecutorDriver) {
	fmt.Println("Executor disconnected.")
}

func (e *DiegoExecutor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	fmt.Println("Launching task", taskInfo.GetName(), "with command", taskInfo.Command.GetValue())

	runStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_STARTING.Enum(),
	}
	_, err := driver.SendStatusUpdate(runStatus)
	if err != nil {
		fmt.Println("Got error", err)
	}

	var works rep.Work
	json.Unmarshal(taskInfo.Data, &works)
	e.cellClient.Perform(works)

	e.lock.Lock()
	defer e.lock.Unlock()
	e.taskStateMap[taskInfo.TaskId.GetValue()] = mesos.TaskState_TASK_STARTING
}

func (e *DiegoExecutor) KillTask(exec.ExecutorDriver, *mesos.TaskID) {
	fmt.Println("Kill task")
}

func (e *DiegoExecutor) FrameworkMessage(driver exec.ExecutorDriver, msg string) {
	fmt.Println("Got framework message: ", msg)
}

func (e *DiegoExecutor) Shutdown(exec.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
}

func (e *DiegoExecutor) Error(driver exec.ExecutorDriver, err string) {
	fmt.Println("Got error message:", err)
}

func (e *DiegoExecutor) watchTasks(driver exec.ExecutorDriver) {
	timeInIdle:= 0 * time.Second
	for {
		select {
		case <- time.After(1*time.Second):
			state, err := e.cellClient.State()
			if (err != nil) {
				fmt.Println("Get rep state error", err)
				break
			}

			repContainerSet := map[string]bool{}
			for _, lrp := range state.LRPs {
				repContainerSet[lrp.Identifier()] = true
			}
			for _, task := range state.Tasks {
				repContainerSet[task.Identifier()] = true
			}

			e.lock.Lock()
			// update task to running status if needed
			for taskId, _:= range repContainerSet {
				taskState := e.taskStateMap[taskId]
				if taskState != mesos.TaskState_TASK_RUNNING {
					_, err := sendTaskStatusUpdate(driver, taskId, mesos.TaskState_TASK_RUNNING)
					if err == nil { e.taskStateMap[taskId] = mesos.TaskState_TASK_RUNNING }
				}
			}

			// find tasks not in cell state
			for taskId, _:= range e.taskStateMap {
				_, exists := repContainerSet[taskId]
				if !exists { // not found, report finished (or failed?)
					_, err := sendTaskStatusUpdate(driver, taskId, mesos.TaskState_TASK_FINISHED)
					if err == nil { delete(e.taskStateMap, taskId) }
				}
			}

			fmt.Println("repContainerSet: ", repContainerSet)
			fmt.Println("taskStateMap: ", e.taskStateMap)

			// nothing running, abort if been idle for a while
			if len(state.LRPs) == 0 && len(state.Tasks) == 0 && len(e.taskStateMap) == 0 {
				timeInIdle += (1*time.Second)
				if timeInIdle >= 5*time.Second {
					driver.Abort()
				}
			} else {
				timeInIdle = 0
			}

			e.lock.Unlock()
		}
	}
}

func sendTaskStatusUpdate(driver exec.ExecutorDriver, taskId string, state mesos.TaskState) (mesos.Status, error) {
	taskStatus := mesos.TaskStatus{
		TaskId: &mesos.TaskID{Value: proto.String(taskId)},
		State:  &state,
	}
	driverStatus, err := driver.SendStatusUpdate(&taskStatus)
	if err != nil {
		fmt.Printf("Send task status error, driverStatus: %v, err: %v\n", driverStatus.String(), err)
	}
	return driverStatus, err
}
