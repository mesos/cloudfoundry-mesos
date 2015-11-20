package auctionrunner

import (
	"os"
	"sync"
	"time"

	log "github.com/golang/glog"

	sched "github.com/mesos/mesos-go/scheduler"
	"github.com/mesos/cloudfoundry-mesos/scheduler"

	"github.com/pivotal-golang/clock"
	"github.com/pivotal-golang/lager"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auctioneer"
	"github.com/cloudfoundry-incubator/bbs/models"
	"github.com/cloudfoundry-incubator/rep"
	"github.com/cloudfoundry/gunk/workpool"
)

type AuctionRunner struct {
	delegate       auctiontypes.AuctionRunnerDelegate
	metricEmitter  auctiontypes.AuctionMetricEmitterDelegate
	clock          clock.Clock
	workPool       *workpool.WorkPool // not used
	logger         lager.Logger // not used

	lrpAuctions    []auctiontypes.LRPAuction
	taskAuctions   []auctiontypes.TaskAuction
	lock           sync.RWMutex
	HasWork        chan struct{}

	scheduler      *scheduler.DiegoScheduler
	driver *sched.MesosSchedulerDriver
}

func New(
	delegate auctiontypes.AuctionRunnerDelegate,
	metricEmitter auctiontypes.AuctionMetricEmitterDelegate,
	clock clock.Clock,
	workPool *workpool.WorkPool,
	logger lager.Logger,
) *AuctionRunner {
	scheduler, driver := scheduler.InitializeScheduler()

	return &AuctionRunner{
		delegate:      delegate,
		metricEmitter: metricEmitter,
		clock:         clock,
		workPool:      workPool,
		logger:        logger,
		HasWork:        make(chan struct{}, 1),
		scheduler:     scheduler,
		driver:        driver,
	}
}

func (a *AuctionRunner) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	close(ready)
	status, err := a.driver.Start()
	if (err != nil) {
		log.Fatalf("failed to start mesos scheduler driver, status: %s, err: %v\n", status, err)
		return err
	}

	for {
		select {
		case <- a.HasWork:
			a.scheduler.HoldOffers()
			lrpAuctions, taskAuctions := a.collectAuctions(1 * time.Second)
			auctionResults := a.scheduler.Schedule(lrpAuctions, taskAuctions)

			log.Infof("scheduled successful lrps: %d, successful tasks: %d, failed lrps: %d, failed tasks: %d\n",
				len(auctionResults.SuccessfulLRPs),	len(auctionResults.SuccessfulTasks),
				len(auctionResults.FailedLRPs), len(auctionResults.FailedTasks))

			a.metricEmitter.AuctionCompleted(auctionResults)
			a.delegate.AuctionCompleted(auctionResults)
		case <-signals:
			a.driver.Abort()
			return nil
		}
	}
}

func (a *AuctionRunner) ScheduleLRPsForAuctions(lrpStarts []auctioneer.LRPStartRequest) {
	a.lock.Lock()
	defer a.lock.Unlock()

	now := a.clock.Now()
	for _, start := range lrpStarts {
		log.Infof("+lrp auction posted: %v: %v\n", start.ProcessGuid, start.Indices)
		for _, i := range start.Indices {
			lrpKey := models.NewActualLRPKey(start.ProcessGuid, int32(i), start.Domain)
			auction := auctiontypes.NewLRPAuction(rep.NewLRP(lrpKey, start.Resource), now)
			a.lrpAuctions = append(a.lrpAuctions, auction)
		}
	}
	a.claimToHaveWork()
}

func (a *AuctionRunner) ScheduleTasksForAuctions(tasks []auctioneer.TaskStartRequest) {
	a.lock.Lock()
	defer a.lock.Unlock()

	now := a.clock.Now()
	for _, task := range tasks {
		log.Infof("+task auction posted: %v: %v\n", task.TaskGuid)
		auction := auctiontypes.NewTaskAuction(task.Task, now)
		a.taskAuctions = append(a.taskAuctions, auction)
	}
	a.claimToHaveWork()
}

func (a *AuctionRunner) claimToHaveWork() {
	select {
	case a.HasWork <- struct{}{}:
	default:
	}
}

func (a *AuctionRunner) collectAuctions(timeout time.Duration) (
		lrpAuctions []auctiontypes.LRPAuction, taskAuctions []auctiontypes.TaskAuction) {
	if timeout > 0 { time.Sleep(timeout) }

	a.lock.Lock()
	defer a.lock.Unlock()

	presentLRPAuctions := map[string]bool{}
	presentTaskAuctions := map[string]bool{}
	for _, lrpAuction := range a.lrpAuctions {
		id := lrpAuction.Identifier()
		if !presentLRPAuctions[id] {
			lrpAuctions = append(lrpAuctions, lrpAuction)
		}
		presentLRPAuctions[id] = true
	}
	for _, taskAuction := range a.taskAuctions {
		id := taskAuction.Identifier()
		if !presentTaskAuctions[id] {
			taskAuctions = append(taskAuctions, taskAuction)
		}
		presentTaskAuctions[id] = true
	}

	a.lrpAuctions = nil
	a.taskAuctions = nil

	select {
	case <-a.HasWork:
	default:
	}

	return
}
