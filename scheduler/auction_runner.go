package scheduler

import (
	"os"
	"time"

	log "github.com/golang/glog"

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
	logger         lager.Logger
	// TODO: channels not needed any more, use member variable should be more effeicent
	LrpAuctions    chan auctiontypes.LRPAuction
	TaskAuctions   chan auctiontypes.TaskAuction
	AuctionResults chan auctiontypes.AuctionResults
	HasWork         chan struct{}
}

func NewAuctionRunner(
	delegate auctiontypes.AuctionRunnerDelegate,
	metricEmitter auctiontypes.AuctionMetricEmitterDelegate,
	clock clock.Clock,
	workPool *workpool.WorkPool,
	logger lager.Logger,
) *AuctionRunner {
	return &AuctionRunner{
		delegate:      delegate,
		metricEmitter: metricEmitter,
		clock:         clock,
		workPool:      workPool,
		logger:        logger,
		LrpAuctions:   make(chan auctiontypes.LRPAuction, 100),
		TaskAuctions:  make(chan auctiontypes.TaskAuction, 100),
		AuctionResults: make(chan auctiontypes.AuctionResults, 100),
		HasWork:        make(chan struct{}, 1),
	}
}

func (a *AuctionRunner) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	close(ready)

	for {
		select {
		case auctionResults := <- a.AuctionResults:
			logger := a.logger.Session("auction")
			logger.Info("scheduled", lager.Data{
				"successful-lrp-start-auctions": len(auctionResults.SuccessfulLRPs),
				"successful-task-auctions":      len(auctionResults.SuccessfulTasks),
				"failed-lrp-start-auctions":     len(auctionResults.FailedLRPs),
				"failed-task-auctions":          len(auctionResults.FailedTasks),
			})

			a.metricEmitter.AuctionCompleted(auctionResults)
			a.delegate.AuctionCompleted(auctionResults)
		case <-signals:
			return nil
		}
	}
}

func (a *AuctionRunner) ScheduleLRPsForAuctions(lrpStarts []auctioneer.LRPStartRequest) {
	now := a.clock.Now()
	for _, start := range lrpStarts {
		log.Infof("+lrp auction posted: %v: %v\n", start.ProcessGuid, start.Indices)
		for _, i := range start.Indices {
			lrpKey := models.NewActualLRPKey(start.ProcessGuid, int32(i), start.Domain)
			auction := auctiontypes.NewLRPAuction(rep.NewLRP(lrpKey, start.Resource), now)
			a.LrpAuctions <- auction
		}
	}
	a.claimToHaveWork()
}

func (a *AuctionRunner) ScheduleTasksForAuctions(tasks []auctioneer.TaskStartRequest) {
	now := a.clock.Now()
	for _, task := range tasks {
		log.Infof("+task auction posted: %v: %v\n", task.TaskGuid)
		auction := auctiontypes.NewTaskAuction(task.Task, now)
		a.TaskAuctions <- auction
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
	presentLRPAuctions := map[string]bool{}
	presentTaskAuctions := map[string]bool{}
	for {
		select {
		case lrpAuction := <-a.LrpAuctions:
			id := lrpAuction.Identifier()
			if !presentLRPAuctions[id] {
				lrpAuctions = append(lrpAuctions, lrpAuction)
			}
			presentLRPAuctions[id] = true
		case taskAuction := <-a.TaskAuctions:
			id := taskAuction.Identifier()
			if !presentTaskAuctions[id] {
				taskAuctions = append(taskAuctions, taskAuction)
			}
			presentTaskAuctions[id] = true
		case <-a.HasWork:
		default:
			return
		}
	}
}

func (a *AuctionRunner) writeAuctionResults(results auctiontypes.AuctionResults) {
	if (len(results.SuccessfulLRPs) > 0 || len(results.FailedLRPs) > 0 ||
	len(results.SuccessfulTasks) > 0 || len(results.FailedTasks) > 0) {
		a.AuctionResults <- results;
	}
}