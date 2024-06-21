package term

import (
	"bytes"
	"testing"
)

func TestToBytes(t *testing.T) {
	codes, err := ToBytes("ctrl-a,a")
	if err != nil {
		t.Error(err)
	}
	expected := []byte{1, 97}
	if !bytes.Equal(codes, expected) {
		t.Errorf("expected: %+v, got: %+v", expected, codes)
	}

	_, err = ToBytes("shift-z")
	if err == nil {
		t.Error("expected and error")
	}

	codes, err = ToBytes("ctrl-@,ctrl-[,~,ctrl-o")
	if err != nil {
		t.Error(err)
	}
	expected = []byte{0, 27, 126, 15}
	if !bytes.Equal(codes, expected) {
		t.Errorf("expected: %+v, got: %+v", expected, codes)
	}

	codes, err = ToBytes("DEL,+")
	if err != nil {
		t.Error(err)
	}
	expected = []byte{127, 43}
	if !bytes.Equal(codes, expected) {
		t.Errorf("expected: %+v, got: %+v", expected, codes)
	}
}
