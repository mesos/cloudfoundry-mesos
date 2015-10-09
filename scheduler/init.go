package scheduler

import (
	"fmt"
	"flag"
	"strings"
	"net"
	"net/http"
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
	address = flag.String("address", "127.0.0.1", "Binding address for artifact server")
	artifactPort = flag.Int("artifactPort", 12345, "Binding port for artifact server")
	executorPath = flag.String("executor", "./executor", "Path to test executor")
	authProvider = flag.String("mesos_authentication_provider", sasl.ProviderName,
		fmt.Sprintf("Authentication provider to use, default is SASL that supports mechanisms: %+v", mech.ListSupported()))
	master              = flag.String("master", "127.0.0.1:5050", "Master address <ip:port>")
	mesosAuthPrincipal  = flag.String("mesos_authentication_principal", "", "Mesos authentication principal.")
	mesosAuthSecretFile = flag.String("mesos_authentication_secret_file", "", "Mesos authentication secret file.")
)

func InitializeScheduler(auctionRunner *AuctionRunner) *SchedulerRunner {
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

	digoScheduler := NewDiegoScheduler(exec, auctionRunner.LrpAuctions, auctionRunner.TaskAuctions, auctionRunner.AuctionResults)
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

	return NewSchedulerRunner(driver)

}

func prepareExecutorInfo() *mesos.ExecutorInfo {
	uri, _:= serveExecutorArtifact(*executorPath)
	executorUris := []*mesos.CommandInfo_URI{
		&mesos.CommandInfo_URI{
			Value: uri,
			//Extract: proto.Bool(true),
		},
	}

	containerType := mesos.ContainerInfo_MESOS
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
					ContainerPath: proto.String("/var/vcap"),
					HostPath: proto.String("vcap"),
				},
			},
		},
		Command: &mesos.CommandInfo {
			Uris: executorUris,
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
			Value: proto.String("./entrypoint.sh"),
			Arguments: []string{ "./executor", "-logtostderr=true", "-v=5" },
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

// returns (downloadURI, basename(path))
func serveExecutorArtifact(path string) (*string, string) {
	serveFile := func(pattern string, filename string) {
		http.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			http.ServeFile(w, r, filename)
		})
	}

	// Create base path (http://foobar:5000/<base>)
	pathSplit := strings.Split(path, "/")
	var base string
	if len(pathSplit) > 0 {
		base = pathSplit[len(pathSplit)-1]
	} else {
		base = path
	}
	serveFile("/"+base, path)

	hostURI := fmt.Sprintf("http://%s:%d/%s", *address, *artifactPort, base)
	log.V(2).Infof("Hosting artifact '%s' at '%s'", path, hostURI)
	go http.ListenAndServe(fmt.Sprintf("%s:%d", *address, *artifactPort), nil)
	log.V(2).Info("Serving executor artifacts...")

	return &hostURI, base
}
