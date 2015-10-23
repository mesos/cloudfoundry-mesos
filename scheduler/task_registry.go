package scheduler

import (
	"sync"

	mesos "github.com/mesos/mesos-go/mesosproto"
)

type slaveInfo struct {
	Lrps map[string]map[int]mesos.TaskState   // slave id -> task id -> task count
	Tasks map[string]mesos.TaskState
}

func newSlaveInfo() *slaveInfo {
	return &slaveInfo{
		Lrps: map[string]map[int]mesos.TaskState{},
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

func (r *TaskRegistry) AddLrp(slaveId string, guid string, index int, state mesos.TaskState) {
	r.lock.Lock()
	defer r.lock.Unlock()

	slaveInfo := r.getOrCreateSlaveInfo(slaveId)
	lrpInfo, exists := slaveInfo.Lrps[guid]
	if !exists {
		lrpInfo = map[int]mesos.TaskState{}
		slaveInfo.Lrps[guid] = lrpInfo
	}
	lrpInfo[index] = state
}

func (r *TaskRegistry) RemoveLrp(slaveId string, guid string, index int) {
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
	r.lock.RLock()
	defer r.lock.RUnlock()

	slaveInfo, exists := r.slaveMap[slaveId]
	if !exists { return false }
	if len(slaveInfo.Lrps) > 0 { return true }
	if len(slaveInfo.Tasks) > 0 { return true }
	return false
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