package scheduler

import (
	"sync"
	"encoding/json"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/rep"
)


type DiegoScheduler struct {
	executor     *mesos.ExecutorInfo

	holdOffer bool
	offers []*mesos.Offer
	offersLock sync.RWMutex

	registry *TaskRegistry

	scheduler SchedulerInterface
	driver sched.SchedulerDriver
}

func NewDiegoScheduler(exec *mesos.ExecutorInfo) *DiegoScheduler {
	registry := NewTaskRegistry()

	var scheduler SchedulerInterface
	if *auctionStrategy == "spread" {
		scheduler = NewSpreadScheduler(registry)
	} else {
		scheduler = NewBinPackScheduler(registry)
	}

	return &DiegoScheduler{
		executor: exec,
		registry: registry,
		scheduler: scheduler,
	}
}

func (s *DiegoScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Registered with Master ", masterInfo)
	s.driver = driver
}
func (s *DiegoScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Re-Registered with Master ", masterInfo)
}
func (s *DiegoScheduler) Disconnected(sched.SchedulerDriver) {
	log.Fatalf("disconnected from master, aborting")
}

func (s *DiegoScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	logOffers(offers)

	s.offersLock.Lock()
	defer s.offersLock.Unlock()

	if s.holdOffer {
		s.offers = append(s.offers, offers...)
	} else {
		offerIds := extractOfferIds(offers)
		driver.LaunchTasks(offerIds, nil, &mesos.Filters{RefuseSeconds: proto.Float64(30)})
	}
}

func (s *DiegoScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), "on slave ", status.SlaveId.GetValue(), " is in state ", status.State.Enum().String())
	log.Infof("--source: %v, reason: %v, message: %s", status.Source.Enum().String(),
		func(reason *mesos.TaskStatus_Reason) string { if reason == nil { return "<nil>" } else { return status.Reason.String() } }(status.Reason),
		func(message *string) string { if message == nil { return "<nil>" } else { return *message } }(status.Message))

	guid, index := guidFromTaskId(status.TaskId.GetValue())
	switch *status.State {
	case mesos.TaskState_TASK_STAGING, mesos.TaskState_TASK_STARTING, mesos.TaskState_TASK_RUNNING:
		if index >= 0 {
			s.registry.AddLrp(status.SlaveId.GetValue(), guid, index, *status.State)
		} else {
			s.registry.AddTask(status.SlaveId.GetValue(), guid, *status.State)
		}
	case mesos.TaskState_TASK_ERROR:
		// this happens when launching task too slow while diego re-initiated the auction
		// task will fail due to duplicated task id
		// in this case do not remove task from registry
		// or the task launch simply failed where it has not been put into registry
		// TODO: should not launch task in such case
		if *status.Source == mesos.TaskStatus_SOURCE_MASTER { break }
		fallthrough
	default:
		if index >= 0 {
			s.registry.RemoveLrp(status.SlaveId.GetValue(), guid, index)
		} else {
			s.registry.RemoveTask(status.SlaveId.GetValue(), guid)
		}
	}
}

func (s *DiegoScheduler) OfferRescinded(_ sched.SchedulerDriver, oid *mesos.OfferID) {
	log.Errorf("offer rescinded: %v", oid)
}
func (s *DiegoScheduler) FrameworkMessage(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, msg string) {
	log.Errorf("framework message from executor %q slave %q: %q", eid, sid, msg)
}
func (s *DiegoScheduler) SlaveLost(_ sched.SchedulerDriver, sid *mesos.SlaveID) {
	log.Errorf("slave lost: %v", sid)
}
func (s *DiegoScheduler) ExecutorLost(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, code int) {
	log.Errorf("executor %q lost on slave %q code %d", eid, sid, code)

	// TODO
}
func (s *DiegoScheduler) Error(_ sched.SchedulerDriver, err string) {
	log.Errorf("Scheduler received error:", err)
}

func (s *DiegoScheduler) Schedule(lrpAuctions []auctiontypes.LRPAuction, taskAuctions []auctiontypes.TaskAuction) auctiontypes.AuctionResults {
	s.offersLock.Lock()
	defer s.offersLock.Unlock()

	matches := s.scheduler.MatchOffers(s.offers, lrpAuctions, taskAuctions)
	results := s.scheduleMatched(s.driver, matches)

	s.offers = nil
	s.holdOffer = false

	return results
}
func (s *DiegoScheduler) scheduleMatched(driver sched.SchedulerDriver, matches map[string]*OfferMatch) auctiontypes.AuctionResults {
	results := auctiontypes.AuctionResults{}

	for slaveId, match := range(matches) {
		if (slaveId != "") {
			offers := match.Offers

			taskInfos := []*mesos.TaskInfo{}
			for _, lrpAuction := range(match.LrpAuctions) {
				taskInfo := s.createLrpTaskInfo(util.NewSlaveID(slaveId), &lrpAuction)
				taskInfos = append(taskInfos, taskInfo)
				results.SuccessfulLRPs = append(results.SuccessfulLRPs, lrpAuction)
				log.Infof("+scheduled lrp, lrp: %v/%v mem: %v, offers: mem: %v",
					lrpAuction.ProcessGuid, lrpAuction.Index, lrpAuction.MemoryMB, getOffersMem(offers))
			}
			for _, taskAuction := range(match.TaskAuctions) {
				taskInfo := s.createTaskTaskInfo(util.NewSlaveID(slaveId), &taskAuction)
				taskInfos = append(taskInfos, taskInfo)
				results.SuccessfulTasks = append(results.SuccessfulTasks, taskAuction)
				log.Infof("+scheduled task, task: %v mem: %v, offers: mem: %v",
					taskAuction.TaskGuid, taskAuction.MemoryMB, getOffersMem(offers))
			}

			driver.LaunchTasks(extractOfferIds(offers), taskInfos, // offer getting declied if no tasks
				&mesos.Filters{RefuseSeconds: proto.Float64(30)})

		} else {
			for _, lrpAuction := range(match.LrpAuctions) {
				results.FailedLRPs = append(results.FailedLRPs, lrpAuction)
				log.Warningf("+schedule lrp failed, lrp: %v/%v mem: %v, offers: mem: %v",
					lrpAuction.GetProcessGuid(), lrpAuction.Index, lrpAuction.MemoryMB, getOffersMem(match.Offers))
			}
			for _, taskAuction := range(match.TaskAuctions) {
				results.FailedTasks = append(results.FailedTasks, taskAuction)
				log.Warningf("+schedule task failed, task: %v mem: %v, offers: mem: %v",
					taskAuction.TaskGuid, taskAuction.MemoryMB, getOffersMem(match.Offers))
			}
		}
	}

	return results
}

func (s *DiegoScheduler) HoldOffers() {
	s.offersLock.Lock()
	if !s.holdOffer {
		s.holdOffer = true
		s.driver.ReviveOffers()
	}
	s.offersLock.Unlock()
}

func (s *DiegoScheduler) createLrpTaskInfo(slaveId *mesos.SlaveID, lrpAuction *auctiontypes.LRPAuction) *mesos.TaskInfo {
	work, _ := json.Marshal(rep.Work{LRPs: []rep.LRP{lrpAuction.LRP}, Tasks: []rep.Task{}})
	taskId := mesos.TaskID{Value: proto.String(lrpAuction.Identifier())}
	taskInfo := mesos.TaskInfo{
		Name: proto.String(lrpAuction.Identifier()),
		TaskId: &taskId,
		SlaveId: slaveId,
		Executor: s.executor,
		Resources: []*mesos.Resource {
			util.NewScalarResource("cpus", taskCpuAllocation), // TODO: ??
			util.NewScalarResource("mem", float64(lrpAuction.MemoryMB)),
			util.NewScalarResource("disk", float64(lrpAuction.DiskMB)),
		},
		Data: work,
	}
	return &taskInfo
}

func (s *DiegoScheduler) createTaskTaskInfo(slaveId *mesos.SlaveID, taskAuction *auctiontypes.TaskAuction) *mesos.TaskInfo {				work, _ := json.Marshal(rep.Work{LRPs: []rep.LRP{}, Tasks: []rep.Task{taskAuction.Task}})
	taskId := mesos.TaskID{Value: proto.String(taskAuction.Identifier())}
	taskInfo := mesos.TaskInfo{
		Name: proto.String(taskAuction.Identifier()),
		TaskId: &taskId,
		SlaveId: slaveId,
		Executor: s.executor,
		Resources: []*mesos.Resource {
			util.NewScalarResource("cpus", taskCpuAllocation),
			util.NewScalarResource("mem", float64(taskAuction.MemoryMB)),
			util.NewScalarResource("disk", float64(taskAuction.DiskMB)),
		},
		Data: work,
	}
	return &taskInfo
}
