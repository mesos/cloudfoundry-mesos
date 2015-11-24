package scheduler_test

import (
	"testing"
	"github.com/mesos/cloudfoundry-mesos/scheduler"
	"github.com/mesos/mesos-go/mesosproto"
)

func TestRegistry(t *testing.T) {
	r := scheduler.NewTaskRegistry()
	slave1 := "test_slave1"
	slave2 := "test_slave2"
	lrp1 := "test_lrp1"
	lrp2 := "test_lrp2"
	task1 := "test_task1"
	task2 := "test_task2"
	task3 := "test_task3"
	r.AddLrp(slave1, lrp1, 0, mesosproto.TaskState_TASK_RUNNING)
	r.AddLrp(slave1, lrp1, 2, mesosproto.TaskState_TASK_RUNNING)
	r.AddLrp(slave1, lrp1, 4, mesosproto.TaskState_TASK_RUNNING)
	r.AddLrp(slave1, lrp2, 0, mesosproto.TaskState_TASK_RUNNING)
	r.AddLrp(slave2, lrp2, 2, mesosproto.TaskState_TASK_RUNNING)
	r.AddLrp(slave2, lrp2, 4, mesosproto.TaskState_TASK_RUNNING)
	r.AddTask(slave1, task1, mesosproto.TaskState_TASK_RUNNING)
	r.AddTask(slave2, task2, mesosproto.TaskState_TASK_RUNNING)
	r.AddTask(slave2, task3, mesosproto.TaskState_TASK_RUNNING)

	if !r.HasLrpOrTask(slave1) { t.Errorf("LrpAndTaskCount(%v): %v", slave1, r.LrpAndTaskCount(slave1)) }
	if !r.HasLrpOrTask(slave2) { t.Errorf("LrpAndTaskCount(%v): %v", slave2, r.LrpAndTaskCount(slave2)) }
	if r.LrpAndTaskCount(slave1) != 5 {
		t.Errorf("LrpAndTaskCount(%v): actual: %v, expect: %v\n", slave1, r.LrpAndTaskCount(slave1), 5)
	}
	if r.LrpAndTaskCount(slave2) != 4 {
		t.Errorf("LrpAndTaskCount(%v): actual: %v, expect: %v\n", slave1, r.LrpAndTaskCount(slave1), 4)
	}
	if r.LrpCount(slave1) != 4 {
		t.Errorf("LrpCount(%v): actual: %v, expect: %v\n", slave1, r.LrpCount(slave1), 4)
	}
	if r.LrpCount(slave2) != 2 {
		t.Errorf("LrpCount(%v): actual: %v, expect: %v\n", slave2, r.LrpCount(slave2), 2)
	}
	if r.LrpInstanceCount(slave1, lrp1) != 3 {
		t.Errorf("LrpInstanceCount(%v, %v): actual: %v, expect: %v\n", slave1, lrp1, r.LrpInstanceCount(slave1, lrp1), 3)
	}
	if r.LrpInstanceCount(slave1, lrp2) != 1 {
		t.Errorf("LrpInstanceCount(%v, %v): actual: %v, expect: %v\n", slave1, lrp2, r.LrpInstanceCount(slave1, lrp2), 1)
	}
	if r.LrpInstanceCount(slave2, lrp1) != 0 {
		t.Errorf("LrpInstanceCount(%v, %v): actual: %v, expect: %v\n", slave2, lrp1, r.LrpInstanceCount(slave2, lrp1), 0)
	}
	if r.LrpInstanceCount(slave2, lrp2) != 2 {
		t.Errorf("LrpInstanceCount(%v, %v): actual: %v, expect: %v\n", slave2, lrp2, r.LrpInstanceCount(slave2, lrp2), 2)
	}

	r.AddLrp(slave1, lrp1, 4, mesosproto.TaskState_TASK_FINISHED)
	r.AddLrp(slave1, lrp2, 0, mesosproto.TaskState_TASK_ERROR)
	r.AddTask(slave1, task1, mesosproto.TaskState_TASK_FAILED)
	if r.LrpAndTaskCount(slave1) != 5 {
		t.Errorf("LrpAndTaskCount(%v): actual: %v, expect: %v\n", slave1, r.LrpAndTaskCount(slave1), 5)
	}
	if r.LrpInstanceCount(slave1, lrp1) != 3 {
		t.Errorf("LrpInstanceCount(%v, %v): actual: %v, expect: %v\n", slave1, lrp1, r.LrpInstanceCount(slave1, lrp1), 3)
	}

	r.RemoveLrp(slave1, lrp1, 0)
	r.RemoveLrp(slave1, lrp1, 2)
	if r.LrpCount(slave1) != 2 {
		t.Errorf("LrpCount(%v): actual: %v, expect: %v\n", slave1, r.LrpCount(slave1), 2)
	}
	if r.LrpInstanceCount(slave1, lrp1) != 1 {
		t.Errorf("LrpInstanceCount(%v, %v): actual: %v, expect: %v\n", slave1, lrp1, r.LrpInstanceCount(slave1, lrp1), 1)
	}
	r.RemoveLrp(slave1, lrp1, 4)
	r.RemoveLrp(slave1, lrp2, 0)
	r.RemoveLrp(slave2, lrp2, 2)
	r.RemoveLrp(slave2, lrp2, 4)
	r.RemoveTask(slave1, task1)
	r.RemoveTask(slave2, task2)
	r.RemoveTask(slave2, task3)

	if r.HasLrpOrTask(slave1) { t.Errorf("LrpAndTaskCount(%v): %v", slave1, r.LrpAndTaskCount(slave1)) }
	if r.HasLrpOrTask(slave2) { t.Errorf("LrpAndTaskCount(%v): %v", slave2, r.LrpAndTaskCount(slave2)) }
	if r.LrpAndTaskCount(slave1) != 0 {
		t.Errorf("LrpAndTaskCount(%v): actual: %v, expect: %v\n", slave1, r.LrpAndTaskCount(slave1), 0)
	}
}