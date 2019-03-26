package spec

import "a.yandex-team.ru/yt/go/yt"

func Map() *Spec {
	spec := &Spec{}
	return spec.Map()
}

func (base *Spec) Map() *Spec {
	s := base.Clone()
	s.Type = yt.OperationMap
	return s
}

func Reduce() *Spec {
	spec := &Spec{}
	return spec.Reduce()
}

func (base *Spec) Reduce() *Spec {
	s := base.Clone()
	s.Type = yt.OperationReduce
	return s
}

func MapReduce() *Spec {
	spec := &Spec{}
	return spec.Reduce()
}

func (base *Spec) MapReduce() *Spec {
	s := base.Clone()
	s.Type = yt.OperationMapReduce
	return s
}

func JoinReduce() *Spec {
	spec := &Spec{}
	return spec.Reduce()
}

func (base *Spec) JoinReduce() *Spec {
	s := base.Clone()
	s.Type = yt.OperationJoinReduce
	return s
}

func Sort() *Spec {
	spec := &Spec{}
	return spec.Reduce()
}

func (base *Spec) Sort() *Spec {
	s := base.Clone()
	s.Type = yt.OperationSort
	return s
}

func Merge() *Spec {
	spec := &Spec{}
	return spec.Reduce()
}

func (base *Spec) Merge() *Spec {
	s := base.Clone()
	s.Type = yt.OperationMerge
	return s
}

func Erase() *Spec {
	spec := &Spec{}
	return spec.Reduce()
}

func (base *Spec) Erase() *Spec {
	s := base.Clone()
	s.Type = yt.OperationErase
	return s
}

func Vanilla() *Spec {
	spec := &Spec{}
	return spec.Reduce()
}

func (base *Spec) Vanilla() *Spec {
	s := base.Clone()
	s.Type = yt.OperationVanilla
	return s
}
