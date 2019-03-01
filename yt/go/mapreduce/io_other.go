// +build !linux

package mapreduce

func (c *jobContext) initPipes(nOutputPipes int) error {
	panic("This should never be called on windows")
}
