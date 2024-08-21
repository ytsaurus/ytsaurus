//go:build !linux
// +build !linux

package mapreduce

func (c *jobContext) initPipes(state *jobState, nOutputPipes int) error {
	panic("this should never be called on windows")
}
