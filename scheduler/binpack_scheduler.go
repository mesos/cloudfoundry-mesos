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
type sortableOffers struct {
	offers []*mesos.Offer
	registry *TaskRegistry
}
func (o sortableOffers) Len() int {
	return len(o.offers)
}
func (o sortableOffers) Swap(i, j int) {
	o.offers[i], o.offers[j] = o.offers[j], o.offers[i]
}
func (o sortableOffers) Less(i, j int) bool {
	// slave that already has task running goes first
	// slave with less resources goes first

	slaveIdI := o.offers[i].SlaveId.GetValue()
	slaveIdJ := o.offers[j].SlaveId.GetValue()
	if o.registry.HasLrpOrTask(slaveIdI) && !o.registry.HasLrpOrTask(slaveIdJ) {
		return true
	}
	if o.registry.HasLrpOrTask(slaveIdJ) && !o.registry.HasLrpOrTask(slaveIdI) {
		return false
	}
	return getOfferMem(o.offers[i]) <= getOfferMem(o.offers[j])
}
////////////////////////////
func NewBinPackScheduler(registry *TaskRegistry) *BinPackScheduler {
	return &BinPackScheduler {
		registry: registry,
	}
}

func (s *BinPackScheduler)MatchOffers(offers []*mesos.Offer, lrpAuctions []auctiontypes.LRPAuction, taskAuctions []auctiontypes.TaskAuction) (map[string]*OfferMatch) {
	sort.Sort(auctionrunner.SortableLRPAuctions(lrpAuctions))
	sort.Sort(auctionrunner.SortableTaskAuctions(taskAuctions))
	sort.Sort(sortableOffers{offers: offers, registry: s.registry})

	matches := make(map[string]*OfferMatch)
	for _, offer := range(offers) { matches[offer.Id.GetValue()] = &OfferMatch{Offer: offer} }
	matches[""] = &OfferMatch{}

	for _, lrpAuction := range lrpAuctions {
		_, offer := matchLrpAuction(offers, &lrpAuction)
		if offer != nil {
			matches[offer.Id.GetValue()].LrpAuctions = append(matches[offer.Id.GetValue()].LrpAuctions, lrpAuction)
		} else {
			matches[""].LrpAuctions = append(matches[""].LrpAuctions, lrpAuction)
		}
	}
	for _, taskAuction := range taskAuctions {
		_, offer := matchTaskAuction(offers, &taskAuction)
		if offer != nil {
			matches[offer.Id.GetValue()].TaskAuctions = append(matches[offer.Id.GetValue()].TaskAuctions, taskAuction)
		} else {
			matches[""].TaskAuctions = append(matches[""].TaskAuctions, taskAuction)
		}
	}

	return matches
}


func matchLrpAuction(offers []*mesos.Offer, lrpAuction *auctiontypes.LRPAuction) (int, *mesos.Offer) {
	lrpAuction.Attempts++
	for i, offer := range offers {
		if getOfferMem(offer) > float64(lrpAuction.MemoryMB) &&
		getOfferDisk(offer) > float64(lrpAuction.DiskMB) {
			lrpAuction.Winner = offer.SlaveId.GetValue();
			lrpAuction.WaitDuration = time.Now().Sub(lrpAuction.QueueTime)
			return i, offer
		}
	}
	lrpAuction.PlacementError = rep.ErrorInsufficientResources.Error()
	return -1, nil
}

func matchTaskAuction(offers []*mesos.Offer, taskAuction *auctiontypes.TaskAuction) (int, *mesos.Offer) {
	taskAuction.Attempts++
	for i, offer := range offers {
		if getOfferMem(offer) > float64(taskAuction.MemoryMB) &&
		getOfferDisk(offer) > float64(taskAuction.DiskMB) {
			taskAuction.Winner = offer.SlaveId.GetValue();
			taskAuction.WaitDuration = time.Now().Sub(taskAuction.QueueTime)
			return i, offer
		}
	}
	taskAuction.PlacementError = rep.ErrorInsufficientResources.Error()
	return -1, nil
}