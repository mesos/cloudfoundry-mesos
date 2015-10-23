package scheduler

import (
	"time"
	"encoding/json"

	"github.com/gogo/protobuf/proto"
	log "github.com/golang/glog"
	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
	sched "github.com/mesos/mesos-go/scheduler"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/rep"
)

type OfferMatch struct {
	Offer *mesos.Offer
	LrpAuctions []auctiontypes.LRPAuction
	TaskAuctions []auctiontypes.TaskAuction
}

type DiegoScheduler struct {
	executor     *mesos.ExecutorInfo
	lrpAuctions  chan auctiontypes.LRPAuction
	taskAuctions chan auctiontypes.TaskAuction
	auctionResults chan auctiontypes.AuctionResults
	registry *TaskRegistry
	scheduler *BinPackScheduler
}

func NewDiegoScheduler(exec *mesos.ExecutorInfo,
	lrpAuctions  chan auctiontypes.LRPAuction, taskAuctions chan auctiontypes.TaskAuction,
	auctionResults chan auctiontypes.AuctionResults,
) *DiegoScheduler {
	registry := NewTaskRegistry()
	return &DiegoScheduler{
		executor: exec,
		lrpAuctions: lrpAuctions,
		taskAuctions: taskAuctions,
		auctionResults: auctionResults,
		registry: registry,
		scheduler: NewBinPackScheduler(registry),
	}
}

func (s *DiegoScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Registered with Master ", masterInfo)
}
func (s *DiegoScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Re-Registered with Master ", masterInfo)
}
func (s *DiegoScheduler) Disconnected(sched.SchedulerDriver) {
	log.Fatalf("disconnected from master, aborting")
}

func (s *DiegoScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	logOffers(offers)
	results := auctiontypes.AuctionResults{}
	lrpAuctions, taskAuctions := s.collectAuctions(0)
	matches := s.scheduler.MatchOffers(offers, lrpAuctions, taskAuctions)

	for offerId, match := range(matches) {
		if (offerId != "") {
			offer := match.Offer

			taskInfos := []*mesos.TaskInfo{}
			for _, lrpAuction := range(match.LrpAuctions) {
				taskInfo := s.createLrpTaskInfo(offer, &lrpAuction)
				taskInfos = append(taskInfos, taskInfo)
				results.SuccessfulLRPs = append(results.SuccessfulLRPs, lrpAuction)
				log.Infof("scheduled lrp, lrp: %v/%v mem: %v, offer: mem: %v",
					lrpAuction.ProcessGuid, lrpAuction.Index, lrpAuction.MemoryMB, getOfferMem(offer))
			}
			for _, taskAuction := range(match.TaskAuctions) {
				taskInfo := s.createTaskTaskInfo(offer, &taskAuction)
				taskInfos = append(taskInfos, taskInfo)
				results.SuccessfulTasks = append(results.SuccessfulTasks, taskAuction)
				log.Infof("scheduled task, task: %v mem: %v, offer: mem: %v",
					taskAuction.TaskGuid, taskAuction.MemoryMB, getOfferMem(offer))
			}

			driver.LaunchTasks([]*mesos.OfferID{offer.Id}, taskInfos, // offer getting declied if no tasks
				&mesos.Filters{RefuseSeconds: proto.Float64(1)})

		} else {
			for _, lrpAuction := range(match.LrpAuctions) {
				results.FailedLRPs = append(results.FailedLRPs, lrpAuction)
				log.Warningf("schedule lrp failed, lrp: %v/%v mem: %v, offer: mem: %v",
					lrpAuction.GetProcessGuid(), lrpAuction.Index, lrpAuction.MemoryMB, getOfferMem(match.Offer))
			}
			for _, taskAuction := range(match.TaskAuctions) {
				results.FailedTasks = append(results.FailedTasks, taskAuction)
				log.Warningf("schedule task failed, task: %v mem: %v, offer: mem: %v",
					taskAuction.TaskGuid, taskAuction.MemoryMB, getOfferMem(match.Offer))
			}
		}
	}

	if (len(results.SuccessfulLRPs) > 0 || len(results.FailedLRPs) > 0 ||
		len(results.SuccessfulTasks) > 0 || len(results.FailedTasks) > 0) {
		s.auctionResults <- results;
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
		// this happens when launching task to slow while diego re-initiated the auction
		// task will fail due to duplicated task id
		// in this case do not remove task from registry
		// or the task launch failed where it has not been put into registry
		// TODO: should not launch task in such case
		if *status.Reason == mesos.TaskStatus_REASON_TASK_INVALID { break }
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

func (s *DiegoScheduler) collectAuctions(timeout time.Duration) (
		lrpAuctions []auctiontypes.LRPAuction, taskAuctions []auctiontypes.TaskAuction) {
	if timeout > 0 { time.Sleep(timeout) }
	for {
		select {
		case lrpAuction := <-s.lrpAuctions:
			lrpAuctions = append(lrpAuctions, lrpAuction)
		case taskAuction := <-s.taskAuctions:
			taskAuctions = append(taskAuctions, taskAuction)
		default:
			return
		}
	}
}

func (s *DiegoScheduler) createLrpTaskInfo(offer *mesos.Offer, lrpAuction *auctiontypes.LRPAuction) *mesos.TaskInfo {
	work, _ := json.Marshal(rep.Work{LRPs: []rep.LRP{lrpAuction.LRP}, Tasks: []rep.Task{}})
	taskId := mesos.TaskID{Value: proto.String(lrpAuction.Identifier())}
	taskInfo := mesos.TaskInfo{
		Name: proto.String(lrpAuction.Identifier()),
		TaskId: &taskId,
		SlaveId: offer.SlaveId,
		Executor: s.executor,
		Resources: []*mesos.Resource {
			util.NewScalarResource("cpus", 0.1), // TODO: ??
			util.NewScalarResource("mem", float64(lrpAuction.MemoryMB)),
			util.NewScalarResource("disk", float64(lrpAuction.DiskMB)),
		},
		Data: work,
	}
	return &taskInfo
}

func (s *DiegoScheduler) createTaskTaskInfo(offer *mesos.Offer, taskAuction *auctiontypes.TaskAuction) *mesos.TaskInfo {				work, _ := json.Marshal(rep.Work{LRPs: []rep.LRP{}, Tasks: []rep.Task{taskAuction.Task}})
	taskId := mesos.TaskID{Value: proto.String(taskAuction.Identifier())}
	taskInfo := mesos.TaskInfo{
		Name: proto.String(taskAuction.Identifier()),
		TaskId: &taskId,
		SlaveId: offer.SlaveId,
		Executor: s.executor,
		Resources: []*mesos.Resource {
			util.NewScalarResource("cpus", 0.1),
			util.NewScalarResource("mem", float64(taskAuction.MemoryMB)),
			util.NewScalarResource("disk", float64(taskAuction.DiskMB)),
		},
		Data: work,
	}
	return &taskInfo
}
