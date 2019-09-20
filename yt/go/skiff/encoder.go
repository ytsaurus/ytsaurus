package skiff

import "bufio"

type Encoder struct {
	w bufio.Writer
}

func (e *Encoder) Write(value interface{}) error {
	panic("implement me")
}
