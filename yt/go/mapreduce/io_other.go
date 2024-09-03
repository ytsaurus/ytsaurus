//go:build !linux
// +build !linux

package mapreduce

func (c *jobContext) initPipes(nOutputPipes int) error {
	panic("this should never be called on windows")
}

func (c *jobContext) createReader(state *jobState) (Reader, error) {
	panic("this should never be called on windows")
}

func (c *jobContext) createWriters() []Writer {
	panic("this should never be called on windows")
}
