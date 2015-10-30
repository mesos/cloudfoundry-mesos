package scheduler

import (
	mesos "github.com/mesos/mesos-go/mesosproto"
	"github.com/cloudfoundry-incubator/auction/auctiontypes"

)

// TODO: how to allocate cpu since no cpu requirement in diego auction
const taskCpuAllocation  = 0.1
// TODO: diego calculates this number from garden network pool
const maxContainersPerCell = 256


type OfferMatch struct {
	Offers []*mesos.Offer
	LrpAuctions []auctiontypes.LRPAuction
	TaskAuctions []auctiontypes.TaskAuction
}

type SchedulerInterface interface {
	MatchOffers(offers []*mesos.Offer,
			lrpAuctions []auctiontypes.LRPAuction, taskAuctions []auctiontypes.TaskAuction) (
			map[string/*slaveId*/]*OfferMatch)
}