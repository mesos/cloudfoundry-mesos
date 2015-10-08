package scheduler

import (
	"os"

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
	LrpAuctions    chan auctiontypes.LRPAuction
	TaskAuctions   chan auctiontypes.TaskAuction
	AuctionResults chan auctiontypes.AuctionResults
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
		for _, i := range start.Indices {
			lrpKey := models.NewActualLRPKey(start.ProcessGuid, int32(i), start.Domain)
			auction := auctiontypes.NewLRPAuction(rep.NewLRP(lrpKey, start.Resource), now)
			a.LrpAuctions <- auction
		}
	}
}

func (a *AuctionRunner) ScheduleTasksForAuctions(tasks []auctioneer.TaskStartRequest) {
	now := a.clock.Now()
	for _, task := range tasks {
		auction := auctiontypes.NewTaskAuction(task.Task, now)
		a.TaskAuctions <- auction
	}
}
