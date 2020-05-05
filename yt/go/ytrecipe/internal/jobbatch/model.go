package jobbatch

import (
	"time"

	"a.yandex-team.ru/yt/go/guid"
	"a.yandex-team.ru/yt/go/mapreduce/spec"
	"a.yandex-team.ru/yt/go/schema"
	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yt"
)

type Job struct {
	GroupKey string    `yson:",key"`
	ID       guid.GUID `yson:",key"`
	JobState []byte    `yson:",omitempty"`
	SpecPart *SpecPart `yson:",omitempty"`

	Operation *Operation  `yson:",omitempty"`
	PickedBy  interface{} `yson:",omitempty"`
	PickedAt  *time.Time  `yson:",omitempty"`
}

type Operation struct {
	ID yt.OperationID

	OutputTable ypath.Path
	StderrTable ypath.Path
	CoreTable   ypath.Path
}

var JobSchema = schema.MustInfer(&Job{})

type SpecPart struct {
	Files []spec.File
}

func mergeSpecs(jobs []Job) *spec.Spec {
	return spec.Vanilla().AddVanillaTask("jobs", len(jobs))
}
