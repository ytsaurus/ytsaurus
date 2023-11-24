package mapreduce

import (
	"encoding/gob"
	"fmt"
	"reflect"
)

var registry = map[string]reflect.Type{}

// Register registers job type.
//
// Value of job is irrelevant.
//
// User must register all job types during initialization.
//
//	type MyJob struct{}
//
//	func init() {
//	    mapreduce.Register(&MyJob{})
//	}
func Register(job Job) {
	gob.Register(job)
	t := reflect.TypeOf(job)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("job type must be pointer to a struct, but got %T", job))
	}

	key := t.PkgPath() + "." + t.Name()
	registry[key] = t
}

// RegisterName registers job type with overridden name.
func RegisterName(name string, job Job) {
	gob.RegisterName(name, job)
	t := reflect.TypeOf(job)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("job type must be pointer to a struct, but got %T", job))
	}

	key := t.PkgPath() + "." + t.Name()
	registry[key] = reflect.TypeOf(job)
}

// RegisterJobPart registers type that might be used as part of the job state.
func RegisterJobPart(state any) {
	gob.Register(state)
}

func NewJob(name string) Job {
	typ, ok := registry[name]
	if !ok {
		return nil
	}

	return reflect.New(typ).Interface().(Job)
}

func jobName(o Job) string {
	t := reflect.TypeOf(o)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	key := t.PkgPath() + "." + t.Name()
	if _, ok := registry[key]; !ok {
		panic(fmt.Sprintf("job %T is not registered", o))
	}

	return key
}
