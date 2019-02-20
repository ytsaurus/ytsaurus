package mapreduce

import (
	"fmt"
	"reflect"
)

var registry = map[string]reflect.Type{}

func Register(o Job) {
	t := reflect.TypeOf(o)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		panic(fmt.Sprintf("job type must be pointer to a struct, but got %T", o))
	}

	key := t.PkgPath() + "." + t.Name()
	registry[key] = t
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
		panic(fmt.Sprintf("operation %T is not registered", o))
	}

	return key
}
