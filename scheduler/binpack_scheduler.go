package scheduler

import (
	"time"
	"sort"

	mesos "github.com/mesos/mesos-go/mesosproto"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auction/auctionrunner"
	"github.com/cloudfoundry-incubator/rep"
)

type BinPackScheduler struct {
	registry *TaskRegistry
}

/////////////////
type binpackSortableOfferAggregates struct {
	offerAggregates [][]*mesos.Offer
	registry *TaskRegistry
}
func (o binpackSortableOfferAggregates) Len() int {
	return len(o.offerAggregates)
}
func (o binpackSortableOfferAggregates) Swap(i, j int) {
	o.offerAggregates[i], o.offerAggregates[j] = o.offerAggregates[j], o.offerAggregates[i]
}
func (o binpackSortableOfferAggregates) Less(i, j int) bool {
	// slave that already has task running goes first
	// slave with less resources goes first

	slaveIdI := o.offerAggregates[i][0].SlaveId.GetValue()
	slaveIdJ := o.offerAggregates[j][0].SlaveId.GetValue()
	if o.registry.HasLrpOrTask(slaveIdI) && !o.registry.HasLrpOrTask(slaveIdJ) {
		return true
	}
	if o.registry.HasLrpOrTask(slaveIdJ) && !o.registry.HasLrpOrTask(slaveIdI) {
		return false
	}
	return getOffersMem(o.offerAggregates[i]) <= getOffersMem(o.offerAggregates[j])
}
////////////////////////////
func NewBinPackScheduler(registry *TaskRegistry) *BinPackScheduler {
	return &BinPackScheduler {
		registry: registry,
	}
}

func (s *BinPackScheduler)MatchOffers(offers []*mesos.Offer, lrpAuctions []auctiontypes.LRPAuction, taskAuctions []auctiontypes.TaskAuction) (map[string/*slaveId*/]*OfferMatch) {
	sort.Sort(auctionrunner.SortableLRPAuctions(lrpAuctions))
	sort.Sort(auctionrunner.SortableTaskAuctions(taskAuctions))
	offerAggregates := aggregateOffersBySlave(offers)
	sort.Sort(binpackSortableOfferAggregates{offerAggregates: offerAggregates, registry: s.registry})

	matches := make(map[string]*OfferMatch)
	for _, offers := range(offerAggregates) { matches[offers[0].SlaveId.GetValue()] = &OfferMatch{Offers: offers} }
	matches[""] = &OfferMatch{}

	for _, lrpAuction := range lrpAuctions {
		_, offers := matchLrpAuction(offerAggregates, &lrpAuction)
		if offers != nil {
			matches[offers[0].SlaveId.GetValue()].LrpAuctions = append(matches[offers[0].SlaveId.GetValue()].LrpAuctions, lrpAuction)
		} else {
			matches[""].LrpAuctions = append(matches[""].LrpAuctions, lrpAuction)
		}
	}
	for _, taskAuction := range taskAuctions {
		_, offers := matchTaskAuction(offerAggregates, &taskAuction)
		if offers != nil {
			matches[offers[0].SlaveId.GetValue()].TaskAuctions = append(matches[offers[0].SlaveId.GetValue()].TaskAuctions, taskAuction)
		} else {
			matches[""].TaskAuctions = append(matches[""].TaskAuctions, taskAuction)
		}
	}

	return matches
}


func matchLrpAuction(offerAggregates [][]*mesos.Offer, lrpAuction *auctiontypes.LRPAuction) (int, []*mesos.Offer) {
	lrpAuction.Attempts++
	for i, offers := range offerAggregates {
		if getOffersMem(offers) > float64(lrpAuction.MemoryMB) &&
		getOffersCpu(offers) > taskCpuAllocation &&
		getOffersDisk(offers) > float64(lrpAuction.DiskMB) {
			lrpAuction.Winner = offers[0].SlaveId.GetValue();
			lrpAuction.WaitDuration = time.Now().Sub(lrpAuction.QueueTime)
			return i, offers
		}
	}
	lrpAuction.PlacementError = rep.ErrorInsufficientResources.Error()
	return -1, nil
}

func matchTaskAuction(offerAggregates [][]*mesos.Offer, taskAuction *auctiontypes.TaskAuction) (int, []*mesos.Offer) {
	taskAuction.Attempts++
	for i, offers := range offerAggregates {
		if getOffersMem(offers) > float64(taskAuction.MemoryMB) &&
		getOffersCpu(offers) > taskCpuAllocation &&
		getOffersDisk(offers) > float64(taskAuction.DiskMB) {
			taskAuction.Winner = offers[0].SlaveId.GetValue();
			taskAuction.WaitDuration = time.Now().Sub(taskAuction.QueueTime)
			return i, offers
		}
	}
	taskAuction.PlacementError = rep.ErrorInsufficientResources.Error()
	return -1, nil
}