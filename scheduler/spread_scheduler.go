package scheduler

import (
	"sort"
	log "github.com/golang/glog"

	mesos "github.com/mesos/mesos-go/mesosproto"

	"github.com/cloudfoundry-incubator/auction/auctiontypes"
	"github.com/cloudfoundry-incubator/auction/auctionrunner"
)


type SpreadScheduler struct {
	registry *TaskRegistry
	pendingRegistry *TaskRegistry
}

/////////////////
// schedule Task to the largest cell
type spreadSortableOfferAggregatesForTask struct {
	offerAggregates [][]*mesos.Offer
	registry *TaskRegistry
	pendingRegistry *TaskRegistry
}
func (o spreadSortableOfferAggregatesForTask) Len() int {
	return len(o.offerAggregates)
}
func (o spreadSortableOfferAggregatesForTask) Swap(i, j int) {
	o.offerAggregates[i], o.offerAggregates[j] = o.offerAggregates[j], o.offerAggregates[i]
}
func (o spreadSortableOfferAggregatesForTask) Less(i, j int) bool {
	slaveIdI := o.offerAggregates[i][0].SlaveId.GetValue()
	slaveIdJ := o.offerAggregates[j][0].SlaveId.GetValue()
	if (o.registry.HasLrpOrTask(slaveIdI) || o.pendingRegistry.HasLrpOrTask(slaveIdI)) &&
		!(o.registry.HasLrpOrTask(slaveIdJ) || o.pendingRegistry.HasLrpOrTask(slaveIdJ)) {
		return true
	}
	if !(o.registry.HasLrpOrTask(slaveIdI) || o.pendingRegistry.HasLrpOrTask(slaveIdI)) &&
		(o.registry.HasLrpOrTask(slaveIdJ) || o.pendingRegistry.HasLrpOrTask(slaveIdJ)) {
		return false
	}
	return getOffersMem(o.offerAggregates[i]) >= getOffersMem(o.offerAggregates[j])
}
// schedule LRP to the 1. largest non cell slave 2. cell with least lrp instance 3. largest cell
type spreadSortableOfferAggregatesForLrp struct {
	offerAggregates [][]*mesos.Offer
	registry *TaskRegistry
	pendingRegistry *TaskRegistry
	lrpGuid string
}
func (o spreadSortableOfferAggregatesForLrp) Len() int {
	return len(o.offerAggregates)
}
func (o spreadSortableOfferAggregatesForLrp) Swap(i, j int) {
	o.offerAggregates[i], o.offerAggregates[j] = o.offerAggregates[j], o.offerAggregates[i]
}
func (o spreadSortableOfferAggregatesForLrp) Less(i, j int) bool {
	slaveIdI := o.offerAggregates[i][0].SlaveId.GetValue()
	slaveIdJ := o.offerAggregates[j][0].SlaveId.GetValue()
	if (o.registry.HasLrpOrTask(slaveIdI) || o.pendingRegistry.HasLrpOrTask(slaveIdI)) &&
		!(o.registry.HasLrpOrTask(slaveIdJ) || o.pendingRegistry.HasLrpOrTask(slaveIdJ)) {
		return false
	}
	if !(o.registry.HasLrpOrTask(slaveIdI) || o.pendingRegistry.HasLrpOrTask(slaveIdI)) &&
		(o.registry.HasLrpOrTask(slaveIdJ) || o.pendingRegistry.HasLrpOrTask(slaveIdJ)) {
		return true
	}
	if !(o.registry.HasLrpOrTask(slaveIdI) || o.pendingRegistry.HasLrpOrTask(slaveIdI)) &&
		!(o.registry.HasLrpOrTask(slaveIdJ) || o.pendingRegistry.HasLrpOrTask(slaveIdJ)) {
		return getOffersMem(o.offerAggregates[i]) >= getOffersMem(o.offerAggregates[j])
	}
	instanceCountI := o.registry.LrpInstanceCount(slaveIdI, o.lrpGuid) + o.pendingRegistry.LrpInstanceCount(slaveIdI, o.lrpGuid)
	instanceCountJ := o.registry.LrpInstanceCount(slaveIdJ, o.lrpGuid) + o.pendingRegistry.LrpInstanceCount(slaveIdJ, o.lrpGuid)
	if instanceCountI == instanceCountJ {
		return getOffersMem(o.offerAggregates[i]) >= getOffersMem(o.offerAggregates[j])
	}
	return instanceCountI < instanceCountJ
}
//////////////////////
func NewSpreadScheduler(registry *TaskRegistry) *SpreadScheduler {
	return &SpreadScheduler {
		registry: registry,
	}
}

func (s *SpreadScheduler)MatchOffers(offers []*mesos.Offer, lrpAuctions []auctiontypes.LRPAuction, taskAuctions []auctiontypes.TaskAuction) (map[string]*OfferMatch) {
	s.pendingRegistry = NewTaskRegistry()

	sort.Sort(auctionrunner.SortableLRPAuctions(lrpAuctions))
	sort.Sort(auctionrunner.SortableTaskAuctions(taskAuctions))
	offerAggregates := aggregateOffersBySlave(offers)

	matches := make(map[string]*OfferMatch)
	for _, offers := range(offerAggregates) { matches[offers[0].SlaveId.GetValue()] = &OfferMatch{Offers: offers} }
	matches[""] = &OfferMatch{}

	for _, lrpAuction := range lrpAuctions {
		sort.Sort(spreadSortableOfferAggregatesForLrp{offerAggregates: offerAggregates, registry: s.registry, pendingRegistry: s.pendingRegistry, lrpGuid: lrpAuction.ProcessGuid})
		log.Info("--offerAggregates: ", offerAggregates)
		_, offers := matchLrpAuction(offerAggregates, &lrpAuction)
		if offers != nil {
			matches[offers[0].SlaveId.GetValue()].LrpAuctions = append(matches[offers[0].SlaveId.GetValue()].LrpAuctions, lrpAuction)
			s.pendingRegistry.AddLrp(offers[0].SlaveId.GetValue(), lrpAuction.ProcessGuid, lrpAuction.Index, mesos.TaskState_TASK_STAGING)
		} else {
			matches[""].LrpAuctions = append(matches[""].LrpAuctions, lrpAuction)
		}
	}
	for _, taskAuction := range taskAuctions {
		sort.Sort(spreadSortableOfferAggregatesForTask{offerAggregates: offerAggregates, registry: s.registry, pendingRegistry: s.pendingRegistry})
		_, offers := matchTaskAuction(offerAggregates, &taskAuction)
		if offers != nil {
			matches[offers[0].SlaveId.GetValue()].TaskAuctions = append(matches[offers[0].SlaveId.GetValue()].TaskAuctions, taskAuction)
			s.pendingRegistry.AddTask(offers[0].SlaveId.GetValue(), taskAuction.TaskGuid, mesos.TaskState_TASK_STAGING)
		} else {
			matches[""].TaskAuctions = append(matches[""].TaskAuctions, taskAuction)
		}
	}


	s.pendingRegistry = nil
	return matches
}
