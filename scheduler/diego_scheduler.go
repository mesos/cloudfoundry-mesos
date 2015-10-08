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

type DiegoScheduler struct {
	executor     *mesos.ExecutorInfo
	lrpAuctions  chan auctiontypes.LRPAuction
	taskAuctions chan auctiontypes.TaskAuction
	auctionResults chan auctiontypes.AuctionResults
}

func NewDiegoScheduler(exec *mesos.ExecutorInfo,
	lrpAuctions  chan auctiontypes.LRPAuction, taskAuctions chan auctiontypes.TaskAuction,
	auctionResults chan auctiontypes.AuctionResults,
) *DiegoScheduler {
	return &DiegoScheduler{
		executor: exec,
		lrpAuctions: lrpAuctions,
		taskAuctions: taskAuctions,
		auctionResults: auctionResults,
	}
}

func (sched *DiegoScheduler) Registered(driver sched.SchedulerDriver, frameworkId *mesos.FrameworkID, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Registered with Master ", masterInfo)
}
func (sched *DiegoScheduler) Reregistered(driver sched.SchedulerDriver, masterInfo *mesos.MasterInfo) {
	log.Infoln("Framework Re-Registered with Master ", masterInfo)
}
func (sched *DiegoScheduler) Disconnected(sched.SchedulerDriver) {
	log.Fatalf("disconnected from master, aborting")
}

func (sched *DiegoScheduler) ResourceOffers(driver sched.SchedulerDriver, offers []*mesos.Offer) {
	logOffers(offers)
	results := auctiontypes.AuctionResults{}

	for _, offer := range offers {
		select {
		case lrp := <- sched.lrpAuctions:
			lrp.Attempts++

			if (float64(lrp.MemoryMB) > getOfferMem(offer) ||
				//float64(lrp.CpuWeight) > getOfferCpu(offer) ||
				float64(lrp.DiskMB) > getOfferDisk(offer)) {
				driver.DeclineOffer(offer.Id, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
				lrp.PlacementError = rep.ErrorInsufficientResources.Error()
				results.FailedLRPs = append(results.FailedLRPs, lrp)
				log.Warningf("schedule lrp failed, lrp: %v-%v mem: %v, offer: %v mem: %v",
					lrp.GetProcessGuid(), lrp.Index, lrp.MemoryMB, offer.Id, getOfferMem(offer))
				break
			}

			lrp.Winner = offer.SlaveId.GetValue()
			work, _ := json.Marshal(rep.Work{LRPs: []rep.LRP{lrp.LRP}, Tasks: []rep.Task{}})
			taskId := mesos.TaskID{Value: proto.String(lrp.Identifier())}
			taskInfo := mesos.TaskInfo{
				Name: proto.String(lrp.Identifier()),
				TaskId: &taskId,
				SlaveId: offer.SlaveId,
				Executor: sched.executor,
				Resources: []*mesos.Resource {
					util.NewScalarResource("cpus", 0.1), // TODO:
					util.NewScalarResource("mem", float64(lrp.MemoryMB)),
					util.NewScalarResource("disk", float64(lrp.DiskMB)),
				},
				Data: work,
			}
			driver.LaunchTasks([]*mesos.OfferID{offer.Id}, []*mesos.TaskInfo{&taskInfo},
				&mesos.Filters{RefuseSeconds: proto.Float64(5)})
			log.Infof("schedule lrp, lrp: %v-%v mem: %v, offer: %v mem: %v",
				lrp.ProcessGuid, lrp.Index, lrp.MemoryMB, offer.Id, getOfferMem(offer))
			lrp.WaitDuration = time.Now().Sub(lrp.QueueTime)
			results.SuccessfulLRPs = append(results.SuccessfulLRPs, lrp)
		case task := <- sched.taskAuctions:
			task.Attempts++
			if (float64(task.MemoryMB) > getOfferMem(offer) ||
				//float64(task.CpuWeight) > getOfferCpu(offer) ||
				float64(task.DiskMB) > getOfferDisk(offer)) {
				driver.DeclineOffer(offer.Id, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
				task.PlacementError = rep.ErrorInsufficientResources.Error()
				results.FailedTasks = append(results.FailedTasks, task)
				log.Warningf("schedule task failed, task: %v mem: %v, offer: %v mem: %v",
					task.Task.TaskGuid, task.MemoryMB, offer.Id, getOfferMem(offer))
				break
			}

			task.Winner = offer.GetHostname()
			work, _ := json.Marshal(rep.Work{LRPs: []rep.LRP{}, Tasks: []rep.Task{task.Task}})
			taskId := mesos.TaskID{Value: proto.String(task.Identifier())}
			taskInfo := mesos.TaskInfo{
				Name: proto.String(task.Identifier()),
				TaskId: &taskId,
				SlaveId: offer.SlaveId,
				Executor: sched.executor,
				Resources: []*mesos.Resource {
					util.NewScalarResource("cpus", 0.1),
					util.NewScalarResource("mem", float64(task.MemoryMB)),
					util.NewScalarResource("disk", float64(task.DiskMB)),
				},
				Data: work,
			}
			driver.LaunchTasks([]*mesos.OfferID{offer.Id}, []*mesos.TaskInfo{&taskInfo},
				&mesos.Filters{RefuseSeconds: proto.Float64(5)})
			log.Infof("schedule task, task: %v mem: %v, offer: %v mem: %v",
				task.Task.TaskGuid, task.MemoryMB, offer.Id, getOfferMem(offer))
			task.WaitDuration = time.Now().Sub(task.QueueTime)
			results.SuccessfulTasks = append(results.SuccessfulTasks, task)

		default:
			driver.DeclineOffer(offer.Id, &mesos.Filters{RefuseSeconds: proto.Float64(1)})
		}
	}

	if (len(results.SuccessfulLRPs) > 0 || len(results.FailedLRPs) > 0 ||
		len(results.SuccessfulTasks) > 0 || len(results.FailedTasks) > 0) {
		sched.auctionResults <- results;
	}
}

func (sched *DiegoScheduler) StatusUpdate(driver sched.SchedulerDriver, status *mesos.TaskStatus) {
	log.Infoln("Status update: task", status.TaskId.GetValue(), "on slave ", status.SlaveId.GetValue(), " is in state ", status.State.Enum().String())
	log.Infof("--source: %v, reason: %v, message: %s", status.Source.Enum().String(),
		func(reason *mesos.TaskStatus_Reason) string { if reason == nil { return "<nil>" } else { return status.Reason.Enum().String() } }(status.Reason),
		func(message *string) string { if message == nil { return "<nil>" } else { return *message } }(status.Message))
}

func (sched *DiegoScheduler) OfferRescinded(_ sched.SchedulerDriver, oid *mesos.OfferID) {
	log.Errorf("offer rescinded: %v", oid)
}
func (sched *DiegoScheduler) FrameworkMessage(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, msg string) {
	log.Errorf("framework message from executor %q slave %q: %q", eid, sid, msg)
}
func (sched *DiegoScheduler) SlaveLost(_ sched.SchedulerDriver, sid *mesos.SlaveID) {
	log.Errorf("slave lost: %v", sid)
}
func (sched *DiegoScheduler) ExecutorLost(_ sched.SchedulerDriver, eid *mesos.ExecutorID, sid *mesos.SlaveID, code int) {
	log.Errorf("executor %q lost on slave %q code %d", eid, sid, code)
}
func (sched *DiegoScheduler) Error(_ sched.SchedulerDriver, err string) {
	log.Errorf("Scheduler received error:", err)
}

