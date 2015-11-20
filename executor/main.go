package main

import (
	"flag"
	"fmt"

	exec "github.com/mesos/mesos-go/executor"
)


func init() {
	flag.Parse()
}

func main() {

	fmt.Println("Starting Diego Executor")

	dconfig := exec.DriverConfig{
		Executor: NewDiegoExecutor(),
	}
	driver, err := exec.NewMesosExecutorDriver(dconfig)

	if err != nil {
		fmt.Println("Unable to create a ExecutorDriver ", err.Error())
	}

	_, err = driver.Start()
	if err != nil {
		fmt.Println("Got error:", err)
		return
	}
	fmt.Println("Executor process has started and running.")
	driver.Join()

}


