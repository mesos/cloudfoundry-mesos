## Cloudfoundry-Mesos

This guide applies to the HEAD of the source tree and currently tested only with the following environment:
* `OpenStack Kilo` as IaaS
* `Ubuntu 14.04` with `Docker 1.8.2` installed as Mesos host
* `Mesos 0.25.0` with Docker containerizer enabled
* `CloudFoundry v219`, `Diego v0.1434.0` and `Garden Linux v0.307.0` 


## Deploy CloudFoundry Diego

Cloudfoundry-Diego is currently tested only with `CloudFoundry v219`, `Diego v0.1434.0` and `Garden Linux v0.307.0`. Please refer to [the Diego release document](https://github.com/cloudfoundry-incubator/diego-release) to deploy the specified version.

CloudFoundry has two run-time environments, `DEA` and `Diego`.
By default the CloudFoundry apps will be deployed in the DEA environment which is currently not supported by CloudFoundry-Mesos.
Please set the Cloud Controller property `default_to_diego_backend` to `true` in the manifest file when deploying CloudFoundry.


## Deploy Mesos

Cloudfoundry-Diego works with Mesos 0.25.0, the host OS should be `Ubuntu 14.04` (Other distributions are not tested) with `Docker 1.8.2` or later installed.

Please follow the [Mesos](http://mesos.apache.org/gettingstarted/) and/or [MESOSPHERE](https://mesosphere.com/downloads/) documents to install and start a Mesos cluster.


## Patch and Build `auctioneer`

Get Diego release code and make sure you can compile `auctioneer` without a problem
``` BASH
$ git clone https://github.com/cloudfoundry-incubator/diego-release.git
$ cd diego-release
$ git checkout v0.1434.0
$ ./scripts/update
$ export GOPATH=$(pwd)
$ cd src/github.com/cloudfoundry-incubator/auctioneer/cmd/auctioneer/
$ go build

```
Patch `auctioneer` to use Mesos's `auctionrunner` package
``` BASH
$ sed -i 's|"github.com/cloudfoundry-incubator/auction/auctionrunner"|"github.com/mesos/cloudfoundry-mesos/scheduler/auctionrunner"|g' main.go
```
Get all the dependencies and rebuild the auctioneer
``` BASH
$ go get ./...
$ go build
```


## Build the Executor

Build the Mesos executor binary is easy
``` BASH
$ cd ../../../../mesos/cloudfoundry-mesos/executor/
$ go build
```
In order to create Diego cells dynamically, this project packs the executor binary together with all the Diego cell jobs in to a big Docker image.
Starting this Docker container will configure and start Diego cell jobs (consul, metron, rep and garden) and also launch the Mesos executor.
Note that in order for Diego's `Garden-Linux` to work, the cell container should be running with `host` networking and in `privileged` mode.

The docker image is current manually created and pushed to Docker hub at `jianhuiz/diego-cell:219-1434-307`. You can pull the image and do the customizations on top of it.
The creation of the image includes compilation and packaging of CloudFoundry/Diego/Garden binaries and patching the startup scripts.
We will automate the image creation and publish the details at a later time.

## Replace the `auctioneer`

Find the IP of Diego `brain` VM by running `bosh vms`, this is by default where the `auctioneer` runs. SSH into that host and stop the Diego auctioneer job.
``` BASH
$ sudo monit stop auctioneer
```
Replace the auctioneer binary file `/var/vcap/packages/auctioneer/bin/auctioneer` with the newly compiled one and patch the auctioneer starting script `/var/vcap/jobs/auctioneer/bin/auctioneer_ctl` with the following new parameters added to the auctioneer start command line:
```BASH
      -address 192.168.2.12 \
      -master=zk://192.168.0.15:2181/mesos \
      -auction_strategy=binpack \
      -consul_server=192.168.1.113 \
      -etcd_url=http://192.168.1.109:4001 \
```
Where `-address` is the host IP where the auctioneer runs; `-master` is the Mesos master address; `-auction_strategy` is teh scheduling strategy when doing the auctioning; `-consul_server` and `-etcd_url` will be passed to the dynamically created Diego cell to join the CloudFoundry Cosnul server and access the ETCD storage (Note that there are two ETCDs and this is the ETCD of CloudFoundry, not that of Diego). Please replace those parameter values according to your environment.

Then start the new auctioneer and make sure it's up and running.
```BASH
$ sudo monit start auctioneer
$ sudo monit status | grep -A14 auctioneer
```


## Push CloudFoundry apps

If you have already set `default_to_diego_backend: true` when deploying CloudFoundry, just run `cf push` to deploy and run a CloudFoundry app with Diego runtime. Otherwise please install [Diego cli plugin](https://github.com/cloudfoundry-incubator/diego-cli-plugin) and [specify diego backend](https://github.com/cloudfoundry-incubator/diego-design-notes/blob/master/migrating-to-diego.md#starting-a-new-application-on-diego) when pushing apps.

For test purpose, you can just use the [hello world app](https://github.com/jianhuiz/cf-apps/tree/master/hello). Don't forget to modify the `manifest.yml` accordingly.

Check the app status using `cf app <your-app>` and see the framework and running tasks in Mesos portal. You can also use `cf scale` to change the app instance count and see Mesos tasks created or removed.



