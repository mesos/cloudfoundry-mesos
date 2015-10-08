package executor

import (
	"fmt"
	"time"
	"encoding/json"

	exec "github.com/mesos/mesos-go/executor"
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/cloudfoundry-incubator/cf_http"
	"github.com/pivotal-golang/localip"
	"github.com/cloudfoundry-incubator/rep"
)

type DiegoExecutor struct {
	cellClient rep.Client
}

func NewDiegoExecutor() *DiegoExecutor {
	ip, _ := localip.LocalIP()
	address := fmt.Sprintf("http://%s:%v", ip, 1800)

	repClientFactory := rep.NewClientFactory(cf_http.NewClient())
	cellClient := repClientFactory.CreateClient(address)
	return &DiegoExecutor{
		cellClient: cellClient,
	}
}

func (exec *DiegoExecutor) Registered(driver exec.ExecutorDriver, execInfo *mesos.ExecutorInfo, fwinfo *mesos.FrameworkInfo, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Registered Executor on slave ", slaveInfo.GetHostname())
}

func (exec *DiegoExecutor) Reregistered(driver exec.ExecutorDriver, slaveInfo *mesos.SlaveInfo) {
	fmt.Println("Re-registered Executor on slave ", slaveInfo.GetHostname())
}

func (exec *DiegoExecutor) Disconnected(exec.ExecutorDriver) {
	fmt.Println("Executor disconnected.")
}

func (exec *DiegoExecutor) LaunchTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo) {
	fmt.Println("Launching task", taskInfo.GetName(), "with command", taskInfo.Command.GetValue())

	runStatus := &mesos.TaskStatus{
		TaskId: taskInfo.GetTaskId(),
		State:  mesos.TaskState_TASK_RUNNING.Enum(),
	}
	_, err := driver.SendStatusUpdate(runStatus)
	if err != nil {
		fmt.Println("Got error", err)
	}

	var works rep.Work
	json.Unmarshal(taskInfo.Data, &works)
	exec.cellClient.Perform(works)

	go exec.waitForTask(driver, taskInfo, works)
}

func (exec *DiegoExecutor) KillTask(exec.ExecutorDriver, *mesos.TaskID) {
	fmt.Println("Kill task")
}

func (exec *DiegoExecutor) FrameworkMessage(driver exec.ExecutorDriver, msg string) {
	fmt.Println("Got framework message: ", msg)
}

func (exec *DiegoExecutor) Shutdown(exec.ExecutorDriver) {
	fmt.Println("Shutting down the executor")
}

func (exec *DiegoExecutor) Error(driver exec.ExecutorDriver, err string) {
	fmt.Println("Got error message:", err)
}

func (exec *DiegoExecutor) waitForTask(driver exec.ExecutorDriver, taskInfo *mesos.TaskInfo, works rep.Work) {
	for {
		found:
		select {
		case <- time.After(1*time.Second):
			fmt.Println("pooling cell state")
			state, err := exec.cellClient.State()
			if (err != nil) {
				fmt.Println("Get rep state error", err)
				break
			}
			if len(works.LRPs) > 0 {
				for _, lrp := range state.LRPs {
					if lrp.GetProcessGuid() == works.LRPs[0].GetProcessGuid() &&
					lrp.Index == works.LRPs[0].Index {
						break found
					}
				}
			} else if len(works.Tasks) > 0 {
				for _, task := range state.Tasks {
					if task.Identifier() == works.Tasks[0].Identifier() {
						break found
					}
				}
			}
			// not found, report finished or failed
			fmt.Println("Finishing task", taskInfo.GetName())
			finStatus := &mesos.TaskStatus{
				TaskId: taskInfo.GetTaskId(),
				State:  mesos.TaskState_TASK_FINISHED.Enum(),
			}
			_, err = driver.SendStatusUpdate(finStatus)
			if err != nil {
				fmt.Println("Got error", err)
			}
			fmt.Println("Task finished", taskInfo.GetName())
			return
		}
	}
}
