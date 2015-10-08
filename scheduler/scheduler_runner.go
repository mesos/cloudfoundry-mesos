package scheduler

import (
	"os"
	sched "github.com/mesos/mesos-go/scheduler"
)

type SchedulerRunner struct {
	driver *sched.MesosSchedulerDriver
}

func NewSchedulerRunner(driver *sched.MesosSchedulerDriver) *SchedulerRunner {
	return &SchedulerRunner{
		driver: driver,
	}
}


//////////////
func (r *SchedulerRunner) Run(signals <-chan os.Signal, ready chan<- struct{}) error {
	close(ready)
	go r.driver.Run()
	for {
		select {
		case <- signals:
			r.driver.Abort()
			return nil
		}
	}
}