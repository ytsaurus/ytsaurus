package spec

import "go.ytsaurus.tech/yt/go/yt"

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

func (s *Spec) Map() *Spec {
	ss := s.Clone()
	ss.Type = yt.OperationMap
	enableControlAttributes(&ss.JobIO)
	return ss
}

func Reduce() *Spec {
	spec := &Spec{}
	return spec.Reduce()
}

func (s *Spec) Reduce() *Spec {
	ss := s.Clone()
	ss.Type = yt.OperationReduce
	enableControlAttributes(&ss.JobIO)
	return ss
}

func MapReduce() *Spec {
	spec := &Spec{}
	return spec.MapReduce()
}

func (s *Spec) MapReduce() *Spec {
	ss := s.Clone()
	ss.Type = yt.OperationMapReduce
	enableControlAttributes(&ss.MapJobIO)
	enableControlAttributes(&ss.ReduceJobIO)
	// Required for Reduce because this operation
	// does not support indexes (table, row and range)
	disableIndexControlAttributes(ss.ReduceJobIO.ControlAttributes)
	return ss
}

func Sort() *Spec {
	spec := &Spec{}
	return spec.Sort()
}

func (s *Spec) Sort() *Spec {
	ss := s.Clone()
	ss.Type = yt.OperationSort
	return ss
}

func Merge() *Spec {
	spec := &Spec{}
	return spec.Merge()
}

func (s *Spec) Merge() *Spec {
	ss := s.Clone()
	ss.Type = yt.OperationMerge
	return ss
}

func Erase() *Spec {
	spec := &Spec{}
	return spec.Erase()
}

func (s *Spec) Erase() *Spec {
	ss := s.Clone()
	ss.Type = yt.OperationErase
	return ss
}

func Vanilla() *Spec {
	spec := &Spec{}
	return spec.Vanilla()
}

func (s *Spec) AddVanillaTask(name string, jobCount int) *Spec {
	ss := s.Clone()

	if ss.Tasks == nil {
		ss.Tasks = map[string]*UserScript{}
	}

	u, ok := ss.Tasks[name]
	if !ok {
		u = &UserScript{}
		ss.Tasks[name] = u
	}

	u.JobCount = jobCount
	return ss
}

func (s *Spec) Vanilla() *Spec {
	ss := s.Clone()
	ss.Type = yt.OperationVanilla
	return ss
}
