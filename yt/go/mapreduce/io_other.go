// +build !linux

package mapreduce

func (c *jobContext) initPipes(nOutputPipes int) error {
	panic("this should never be called on windows")
}
