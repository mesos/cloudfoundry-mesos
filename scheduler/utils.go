package scheduler

import (
	"strings"
	"strconv"

	log "github.com/golang/glog"

	mesos "github.com/mesos/mesos-go/mesosproto"
	util "github.com/mesos/mesos-go/mesosutil"
)

func getOfferScalar(offer *mesos.Offer, name string) float64 {
	resources := util.FilterResources(offer.Resources, func(res *mesos.Resource) bool {
		return res.GetName() == name
	})

	value := 0.0
	for _, res := range resources {
		value += res.GetScalar().GetValue()
	}

	return value
}

func getOfferCpu(offer *mesos.Offer) float64 {
	return getOfferScalar(offer, "cpus")
}

func getOfferMem(offer *mesos.Offer) float64 {
	return getOfferScalar(offer, "mem")
}

func getOfferDisk(offer *mesos.Offer) float64 {
	return getOfferScalar(offer, "disk")
}

func logOffers(offers []*mesos.Offer) {
	for i, offer := range offers {
		log.Infof("Received Offer[%v] <%v> with cpus=%v mem=%v disk=%v", i,
			offer.Id.GetValue(), getOfferCpu(offer), getOfferMem(offer), getOfferDisk(offer))
	}
}

func getOffersScalar(offers []*mesos.Offer, name string) (result float64) {
	for _, offer := range offers {
		result += getOfferScalar(offer, name)
	}
	return result
}
func getOffersCpu(offers []*mesos.Offer) float64 {
	return getOffersScalar(offers, "cpus")
}

func getOffersMem(offers []*mesos.Offer) float64 {
	return getOffersScalar(offers, "mem")
}

func getOffersDisk(offers []*mesos.Offer) float64 {
	return getOffersScalar(offers, "disk")
}

func aggregateOffersBySlave(offers []*mesos.Offer) [][]*mesos.Offer{
	offersMap := map[string][]*mesos.Offer{}
	for _, offer := range offers {
		offersMap[offer.SlaveId.GetValue()] = append(offersMap[offer.SlaveId.GetValue()], offer)
	}
	offerAggregates := [][]*mesos.Offer{}
	for _, offers := range offersMap {
		offerAggregates = append(offerAggregates, offers)
	}
	return offerAggregates
}

func extractOfferIds(offers []*mesos.Offer) (offerIds []*mesos.OfferID) {
	for _, offer := range offers {
		offerIds = append(offerIds, offer.Id)
	}
	return offerIds
}

func guidFromTaskId(taskId string) (guid string, index int32 /* -1 for diego tasks */) {
	ss := strings.SplitN(taskId, ".", 2)
	guid = ss[0]
	if len(ss) == 2 {
		indexInt, _ := strconv.Atoi(ss[1])
		index = int32(indexInt)
	} else {
		index = -1
	}
	return
}