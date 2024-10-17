package timbertruck

import (
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/admin/timbertruck/pkg/pipelines"
)

func TestDatastore(t *testing.T) {
	dir := t.TempDir()

	ds, err := NewDatastore(path.Join(dir, "db.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() {
		err = ds.Close()
		if err != nil {
			t.Fatalf("Error closing datatstore: %v", err)
		}
	})

	task := Task{
		StreamName:   "foo",
		StagedPath:   "/var/tmp/foo.1.2",
		INode:        100500,
		CreationTime: time.Date(2020, time.September, 9, 17, 30, 0, 0, time.UTC),
		BoundTime:    time.Date(2020, time.September, 21, 12, 30, 0, 0, time.UTC),
		EndPosition:  pipelines.UncompressedFilePosition(100500),
	}
	_, err = ds.AddTask(&task, 10)
	require.NoError(t, err)

	_, err = ds.TaskByPath("/foo/bar")
	require.ErrorIs(t, err, ErrNotFound)

	taskByPath, err := ds.TaskByPath("/var/tmp/foo.1.2")
	require.NoError(t, err)

	require.Equal(t, task, taskByPath)
}

func TestDatastoreGetTask(t *testing.T) {
	dir := t.TempDir()

	ds, err := NewDatastore(path.Join(dir, "db.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() {
		err = ds.Close()
		if err != nil {
			t.Fatalf("Error closing datatstore: %v", err)
		}
	})

	_, err = ds.PeekNextTask("foo")
	require.ErrorIs(t, err, ErrNotFound)

	task1 := Task{
		StreamName:   "foo",
		StagedPath:   "/var/tmp/foo.1",
		INode:        100500,
		CreationTime: time.Date(1992, time.September, 21, 8, 0, 0, 0, time.UTC),
		BoundTime:    time.Date(2020, time.September, 9, 17, 30, 0, 0, time.UTC),
		EndPosition:  pipelines.UncompressedFilePosition(100500),
	}
	_, err = ds.AddTask(&task1, 10)
	require.NoError(t, err)

	peekedTask, err := ds.PeekNextTask("foo")
	require.NoError(t, err)

	require.Equal(t, task1, peekedTask)

	task2 := Task{
		StreamName:   "foo",
		StagedPath:   "/var/tmp/foo.2",
		INode:        100501,
		CreationTime: time.Date(1991, time.September, 21, 8, 0, 0, 0, time.UTC),
		BoundTime:    time.Date(2020, time.September, 9, 17, 30, 0, 0, time.UTC),
		EndPosition:  pipelines.UncompressedFilePosition(100500),
	}
	_, err = ds.AddTask(&task2, 10)
	require.NoError(t, err)

	peekedTask2, err := ds.PeekNextTask("foo")
	require.NoError(t, err)

	require.Equal(t, task2, peekedTask2)

	task3 := Task{
		StreamName:   "foo",
		StagedPath:   "/var/tmp/foo.3",
		INode:        100503,
		CreationTime: time.Date(1993, time.September, 21, 8, 0, 0, 0, time.UTC),
		BoundTime:    time.Date(2021, time.September, 9, 17, 30, 0, 0, time.UTC),
		EndPosition:  pipelines.UncompressedFilePosition(100500),
	}
	_, err = ds.AddTask(&task3, 10)
	require.NoError(t, err)

	peekedTask3, err := ds.PeekNextTask("foo")
	require.NoError(t, err)

	require.Equal(t, task2, peekedTask3)

	tasks, err := ds.ListActiveTasks()
	require.NoError(t, err)

	require.Equal(t, []Task{task1, task2, task3}, tasks)
}

func TestDatastoreCompleteTask(t *testing.T) {
	dir := t.TempDir()

	ds, err := NewDatastore(path.Join(dir, "db.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() {
		err = ds.Close()
		if err != nil {
			t.Fatalf("Error closing datatstore: %v", err)
		}
	})

	task1 := Task{
		StreamName:   "foo",
		StagedPath:   "/var/tmp/foo.1",
		INode:        100500,
		CreationTime: time.Date(1992, time.September, 21, 8, 0, 0, 0, time.UTC),
		BoundTime:    time.Date(2020, time.September, 9, 17, 30, 0, 0, time.UTC),
	}
	_, err = ds.AddTask(&task1, 10)
	require.NoError(t, err)

	tasks, err := ds.ListActiveTasks()
	require.NoError(t, err)

	require.Equal(t, []Task{task1}, tasks)

	err = ds.CompleteTask("/var/tmp/foo.1", time.Date(2020, time.December, 21, 00, 00, 00, 00, time.UTC), nil)
	require.NoError(t, err)

	resTask, err := ds.TaskByPath("/var/tmp/foo.1")
	require.NoError(t, err)

	require.Equal(t, Task{
		StreamName:     "foo",
		StagedPath:     "/var/tmp/foo.1",
		INode:          0,
		CreationTime:   time.Date(1992, time.September, 21, 8, 0, 0, 0, time.UTC),
		BoundTime:      time.Date(2020, time.September, 9, 17, 30, 0, 0, time.UTC),
		CompletionTime: time.Date(2020, time.December, 21, 00, 00, 00, 00, time.UTC),
	}, resTask)

	tasks, err = ds.ListActiveTasks()
	require.NoError(t, err)

	require.Empty(t, tasks)
}

func TestCleanUpCompletedTask(t *testing.T) {
	dir := t.TempDir()

	ds, err := NewDatastore(path.Join(dir, "db.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() {
		err = ds.Close()
		if err != nil {
			t.Fatalf("Error closing datatstore: %v", err)
		}
	})

	task1 := Task{
		StreamName:   "foo",
		StagedPath:   "/var/tmp/foo.1",
		INode:        100500,
		CreationTime: time.Date(1991, time.January, 1, 0, 0, 0, 0, time.UTC),
		BoundTime:    time.Date(1992, time.January, 1, 0, 0, 0, 0, time.UTC),
		EndPosition:  pipelines.UncompressedFilePosition(100500),
	}
	_, err = ds.AddTask(&task1, 10)
	require.NoError(t, err)

	err = ds.CompleteTask("/var/tmp/foo.1", time.Date(1993, time.January, 1, 0, 0, 0, 0, time.UTC), nil)
	require.NoError(t, err)

	task2 := Task{
		StreamName:   "foo",
		StagedPath:   "/var/tmp/foo.2",
		INode:        100501,
		CreationTime: time.Date(1991, time.January, 1, 0, 0, 0, 0, time.UTC),
		BoundTime:    time.Date(1992, time.January, 1, 0, 0, 0, 0, time.UTC),
		EndPosition:  pipelines.UncompressedFilePosition(100500),
	}
	_, err = ds.AddTask(&task2, 10)
	require.NoError(t, err)

	err = ds.CleanupOldCompletedTasks(time.Date(1994, time.January, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	_, err = ds.TaskByPath("/var/tmp/foo.1")
	require.ErrorIs(t, err, ErrNotFound)

	resTask, err := ds.TaskByPath("/var/tmp/foo.2")
	require.NoError(t, err)

	require.Equal(t, task2, resTask)
}

func TestDatastoreCantAddSamePath(t *testing.T) {
	dir := t.TempDir()

	ds, err := NewDatastore(path.Join(dir, "db.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() {
		err = ds.Close()
		if err != nil {
			t.Fatalf("Error closing datatstore: %v", err)
		}
	})

	task1 := Task{
		StreamName:   "foo",
		StagedPath:   "/var/tmp/foo.1",
		INode:        100500,
		CreationTime: time.Date(1992, time.September, 21, 8, 0, 0, 0, time.UTC),
		BoundTime:    time.Date(2020, time.September, 9, 17, 30, 0, 0, time.UTC),
		EndPosition:  pipelines.UncompressedFilePosition(100500),
	}
	_, err = ds.AddTask(&task1, 10)
	require.NoError(t, err)

	task2 := Task{
		StreamName:   "bar",
		StagedPath:   "/var/tmp/foo.1",
		INode:        100501,
		CreationTime: time.Date(1992, time.September, 21, 8, 0, 0, 0, time.UTC),
		BoundTime:    time.Date(2020, time.September, 9, 17, 30, 0, 0, time.UTC),
		EndPosition:  pipelines.UncompressedFilePosition(100500),
	}
	_, err = ds.AddTask(&task2, 10)
	require.ErrorContains(t, err, "UNIQUE constraint")
}

func TestDatastoreTaskByIno(t *testing.T) {
	dir := t.TempDir()

	ds, err := NewDatastore(path.Join(dir, "db.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() {
		err = ds.Close()
		if err != nil {
			t.Fatalf("Error closing datatstore: %v", err)
		}
	})

	_, err = ds.PeekNextTask("foo")
	require.ErrorIs(t, err, ErrNotFound)

	createTime := time.Date(1992, time.September, 21, 8, 0, 0, 0, time.UTC)
	task1 := Task{
		StreamName:   "foo",
		StagedPath:   "/var/tmp/foo.1",
		INode:        100500,
		CreationTime: createTime,
		EndPosition:  pipelines.UncompressedFilePosition(100500),
	}
	_, err = ds.AddTask(&task1, 10)
	require.NoError(t, err)

	peekedTask, err := ds.ActiveTaskByIno(100500, "foo")
	require.NoError(t, err)

	require.Equal(t, task1, peekedTask)
}

func TestDatastoreTaskLimit(t *testing.T) {
	dir := t.TempDir()

	ds, err := NewDatastore(path.Join(dir, "db.sqlite"))
	require.NoError(t, err)
	t.Cleanup(func() {
		err = ds.Close()
		if err != nil {
			t.Fatalf("Error closing datatstore: %v", err)
		}
	})

	createTask := func(taskIndex int) *Task {
		return &Task{
			StreamName:   "foo",
			StagedPath:   fmt.Sprintf("/var/tmp/foo.%v", taskIndex),
			INode:        100500 + int64(taskIndex),
			CreationTime: time.Date(1992, time.September, 21, 8, 0, 0, 0, time.UTC).Add(time.Duration(taskIndex) * time.Hour),
		}
	}

	warns, err := ds.AddTask(createTask(1), 6)
	require.NoError(t, err)
	require.Empty(t, warns)

	warns, err = ds.AddTask(createTask(2), 6)
	require.NoError(t, err)
	require.Empty(t, warns)

	warns, err = ds.AddTask(createTask(3), 6)
	require.NoError(t, err)
	require.Empty(t, warns)

	warns, err = ds.AddTask(createTask(4), 6)
	require.NoError(t, err)
	require.Empty(t, warns)

	warns, err = ds.AddTask(createTask(5), 6)
	require.NoError(t, err)
	require.Len(t, warns, 1)
	require.ErrorContains(t, warns[0], "limit of the active tasks will be reached soon")

	warns, err = ds.AddTask(createTask(6), 7)
	require.NoError(t, err)
	require.Len(t, warns, 1)
	require.ErrorContains(t, warns[0], "limit of the active tasks will be reached soon")

	warns, err = ds.AddTask(createTask(7), 7)
	require.NoError(t, err)
	require.Len(t, warns, 1)
	require.ErrorContains(t, warns[0], "limit of the active tasks will be reached soon")

	warns, err = ds.AddTask(createTask(8), 7)
	require.ErrorIs(t, err, ErrTaskLimitExceeded)
	require.Empty(t, warns)
}
