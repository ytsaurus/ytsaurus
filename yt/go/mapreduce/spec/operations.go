package spec

import "a.yandex-team.ru/yt/go/yt"

func Map() *Spec {
	spec := &Spec{}
	return spec.Map()
}

func enableControlAttributes(io **JobIO) {
	if *io == nil {
		*io = &JobIO{}
	}

	(*io).ControlAttributes = &ControlAttributes{
		EnableTableIndex: true,
		EnableRowIndex:   true,
		EnableRangeIndex: true,
		EnableKeySwitch:  true,
	}
}

// Set EnableXxxIndex ControlAttributes to false
//
// This function is required for reduce when we start MapReduce operation
func disableIndexControlAttributes(ca *ControlAttributes) {
	ca.EnableTableIndex = false
	ca.EnableRowIndex = false
	ca.EnableRangeIndex = false
}

func (base *Spec) Map() *Spec {
	s := base.Clone()
	s.Type = yt.OperationMap
	enableControlAttributes(&s.JobIO)
	return s
}

func Reduce() *Spec {
	spec := &Spec{}
	return spec.Reduce()
}

func (base *Spec) Reduce() *Spec {
	s := base.Clone()
	s.Type = yt.OperationReduce
	enableControlAttributes(&s.JobIO)
	return s
}

func MapReduce() *Spec {
	spec := &Spec{}
	return spec.Reduce()
}

func (base *Spec) MapReduce() *Spec {
	s := base.Clone()
	s.Type = yt.OperationMapReduce
	enableControlAttributes(&s.MapJobIO)
	enableControlAttributes(&s.ReduceJobIO)
	// Required for Reduce because this operation
	// does not support indexes (table, row and range)
	disableIndexControlAttributes(s.ReduceJobIO.ControlAttributes)
	return s
}

func JoinReduce() *Spec {
	spec := &Spec{}
	return spec.JoinReduce()
}

func (base *Spec) JoinReduce() *Spec {
	s := base.Clone()
	s.Type = yt.OperationJoinReduce
	enableControlAttributes(&s.JobIO)
	return s
}

func Sort() *Spec {
	spec := &Spec{}
	return spec.Sort()
}

func (base *Spec) Sort() *Spec {
	s := base.Clone()
	s.Type = yt.OperationSort
	return s
}

func Merge() *Spec {
	spec := &Spec{}
	return spec.Merge()
}

func (base *Spec) Merge() *Spec {
	s := base.Clone()
	s.Type = yt.OperationMerge
	return s
}

func Erase() *Spec {
	spec := &Spec{}
	return spec.Erase()
}

func (base *Spec) Erase() *Spec {
	s := base.Clone()
	s.Type = yt.OperationErase
	return s
}

func Vanilla() *Spec {
	spec := &Spec{}
	return spec.Vanilla()
}

func (base *Spec) AddVanillaTask(name string, jobCount int) *Spec {
	s := base.Clone()

	if s.Tasks == nil {
		s.Tasks = map[string]*UserScript{}
	}

	u, ok := s.Tasks[name]
	if !ok {
		u = &UserScript{}
		s.Tasks[name] = u
	}

	u.JobCount = jobCount
	return s
}

func (base *Spec) Vanilla() *Spec {
	s := base.Clone()
	s.Type = yt.OperationVanilla
	return s
}
