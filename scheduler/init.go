package scheduler

import (
	"fmt"
	"flag"
	"net"
	"io/ioutil"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	mesos "github.com/mesos/mesos-go/mesosproto"
	sched "github.com/mesos/mesos-go/scheduler"
	"github.com/mesos/mesos-go/auth"
	"github.com/mesos/mesos-go/auth/sasl"
	"github.com/mesos/mesos-go/auth/sasl/mech"
	util "github.com/mesos/mesos-go/mesosutil"
	"github.com/gogo/protobuf/proto"
)

var (
	consulServer = flag.String("consul_server", "", "CloudFoundry Consul server to join")
	etcdUrl = flag.String("etcd_url", "", "CloudFoundry ETCD URL")
	auctionStrategy = flag.String("auction_strategy", "binpack", "<binpadk|spread> Strategy when scheduling auctions")
	address = flag.String("address", "127.0.0.1", "Binding address for artifact server")
	authProvider = flag.String("mesos_authentication_provider", sasl.ProviderName,
		fmt.Sprintf("Authentication provider to use, default is SASL that supports mechanisms: %+v", mech.ListSupported()))
	master              = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
	mesosAuthPrincipal  = flag.String("mesos_authentication_principal", "", "Mesos authentication principal.")
	mesosAuthSecretFile = flag.String("mesos_authentication_secret_file", "", "Mesos authentication secret file.")
)

func InitializeScheduler() (*DiegoScheduler, *sched.MesosSchedulerDriver) {
	exec := prepareExecutorInfo()
	fwinfo := &mesos.FrameworkInfo{
		User: proto.String(""), // Mesos-go will fill in user.
		Name: proto.String("Diego Scheduler"),
	}

	cred := (*mesos.Credential)(nil)
	if *mesosAuthPrincipal != "" {
		fwinfo.Principal = proto.String(*mesosAuthPrincipal)
		secret, err := ioutil.ReadFile(*mesosAuthSecretFile)
		if err != nil {
			log.Fatal(err)
		}
		cred = &mesos.Credential{
			Principal: proto.String(*mesosAuthPrincipal),
			Secret:    secret,
		}
	}
	bindingAddress := parseIP(*address)

	digoScheduler := NewDiegoScheduler(exec)
	config := sched.DriverConfig{
		Scheduler:      digoScheduler,
		Framework:      fwinfo,
		Master:         *master,
		Credential:     cred,
		BindingAddress: bindingAddress,
		WithAuthContext: func(ctx context.Context) context.Context {
			ctx = auth.WithLoginProvider(ctx, *authProvider)
			ctx = sasl.WithBindingAddress(ctx, bindingAddress)
			return ctx
		},
	}
	driver, err := sched.NewMesosSchedulerDriver(config)

	if err != nil {
		log.Fatal("Unable to create a SchedulerDriver ", err.Error())
	}

	return digoScheduler, driver

}

func prepareExecutorInfo() *mesos.ExecutorInfo {

	containerType := mesos.ContainerInfo_DOCKER
	containerNetwork := mesos.ContainerInfo_DockerInfo_HOST
	vcapDataVolumeMode := mesos.Volume_RW
	return &mesos.ExecutorInfo{
		ExecutorId: util.NewExecutorID("diego-executor"),
		Name:       proto.String("Diego Executor"),
		Source:     proto.String("diego-executor"),
		Container: &mesos.ContainerInfo{
			Type: &containerType,
			Volumes: []*mesos.Volume {
				&mesos.Volume{
					Mode: &vcapDataVolumeMode,
					ContainerPath: proto.String("/var/vcap/data"),
					HostPath: proto.String("data"),
				},
				&mesos.Volume{
					Mode: &vcapDataVolumeMode,
					ContainerPath: proto.String("/var/vcap/sys/log"),
					HostPath: proto.String("log"),
				},
				&mesos.Volume{
					Mode: &vcapDataVolumeMode,
					ContainerPath: proto.String("/sys/fs/cgroup"),
					HostPath: proto.String("/sys/fs/cgroup"),
				},
			},
			Docker: &mesos.ContainerInfo_DockerInfo{
				Image: proto.String("jianhuiz/diego-cell"),
				Network: &containerNetwork,
				Privileged: proto.Bool(true),
				ForcePullImage: proto.Bool(true),
			},
		},
		Command: &mesos.CommandInfo {
			Environment: &mesos.Environment{
				Variables: []*mesos.Environment_Variable {
					&mesos.Environment_Variable{
						Name: proto.String("CONSUL_SERVER"),
						Value: proto.String(*consulServer),
					},
					&mesos.Environment_Variable{
						Name: proto.String("ETCD_URL"),
						Value: proto.String(*etcdUrl),
					},
				},
			},
			Shell: proto.Bool(false),
			Value: proto.String("/executor"),
			Arguments: []string{ "-logtostderr=true" },
		},
	}
}

func parseIP(address string) net.IP {
	addr, err := net.LookupIP(address)
	if err != nil {
		log.Fatal(err)
	}
	if len(addr) < 1 {
		log.Fatalf("failed to parse IP from address '%v'", address)
	}
	return addr[0]
}
