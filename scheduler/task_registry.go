package scheduler

import (
	"sync"

	mesos "github.com/mesos/mesos-go/mesosproto"
)

type slaveInfo struct {
	Lrps map[string]map[int32]mesos.TaskState   // lrp guid => lrp index => state
	Tasks map[string]mesos.TaskState
}

func newSlaveInfo() *slaveInfo {
	return &slaveInfo{
		Lrps: map[string]map[int32]mesos.TaskState{},
		Tasks: map[string]mesos.TaskState{},
	}
}

type TaskRegistry struct {
	lock sync.RWMutex
	slaveMap map[string] *slaveInfo
}

func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		slaveMap: map[string] *slaveInfo{},
	}
}

func (r *TaskRegistry) AddLrp(slaveId string, guid string, index int32, state mesos.TaskState) {
	r.lock.Lock()
	defer r.lock.Unlock()

	slaveInfo := r.getOrCreateSlaveInfo(slaveId)
	lrpInfo, exists := slaveInfo.Lrps[guid]
	if !exists {
		lrpInfo = map[int32]mesos.TaskState{}
		slaveInfo.Lrps[guid] = lrpInfo
	}
	lrpInfo[index] = state
}

func (r *TaskRegistry) RemoveLrp(slaveId string, guid string, index int32) {
	r.lock.Lock()
	defer r.lock.Unlock()
	slaveInfo := r.getOrCreateSlaveInfo(slaveId)
	lrpInfo, exists := slaveInfo.Lrps[guid]
	if exists {
		delete(lrpInfo, index)
	}
}

func (r *TaskRegistry) AddTask(slaveId string, guid string, state mesos.TaskState) {
	r.lock.Lock()
	defer r.lock.Unlock()

	slaveInfo := r.getOrCreateSlaveInfo(slaveId)
	slaveInfo.Tasks[guid] = state
}

func (r *TaskRegistry) RemoveTask(slaveId string, guid string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	slaveInfo := r.getOrCreateSlaveInfo(slaveId)
	delete(slaveInfo.Tasks, guid)
}

func (r *TaskRegistry) HasLrpOrTask(slaveId string) bool {
	return r.LrpAndTaskCount(slaveId) > 0
}

func (r *TaskRegistry) LrpCount(slaveId string) int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	slaveInfo, exists := r.slaveMap[slaveId]
	if !exists { return 0 }

	taskCount := 0
	for _, instanceMap := range slaveInfo.Lrps {
		taskCount = taskCount + len(instanceMap)
	}
	return taskCount

}
func (r *TaskRegistry) LrpAndTaskCount(slaveId string) int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	slaveInfo, exists := r.slaveMap[slaveId]
	if !exists { return 0 }

	taskCount := len(slaveInfo.Tasks)
	for _, instanceMap := range slaveInfo.Lrps {
		taskCount = taskCount + len(instanceMap)
	}
	return taskCount
}

func (r *TaskRegistry) LrpInstanceCount(slaveId string, lrpGuid string) int {
	r.lock.RLock()
	defer r.lock.RUnlock()

	slaveInfo, exists := r.slaveMap[slaveId]
	if !exists { return 0 }

	return len(slaveInfo.Lrps[lrpGuid])
}


//////////////////////////////////////
func (r *TaskRegistry) getOrCreateSlaveInfo(slaveId string) *slaveInfo {
	slaveInfo, exists := r.slaveMap[slaveId]
	if !exists {
		slaveInfo = newSlaveInfo()
		r.slaveMap[slaveId] = slaveInfo
	}
	return slaveInfo
}