package fuzz

import (
	"bytes"

	"a.yandex-team.ru/yt/go/yson"
)

func Fuzz(data []byte) int {
	var val interface{}
	err := yson.Unmarshal(data, &val)

	if err != nil {
		return 0
	}

	for _, format := range []yson.Format{yson.FormatPretty, yson.FormatText, yson.FormatBinary} {
		var buf bytes.Buffer
		w := yson.NewWriterFormat(&buf, format)
		enc := yson.NewEncoderWriter(w)

		err = enc.Encode(val)
		if err != nil {
			panic(err)
		}

		var other interface{}
		err = yson.Unmarshal(buf.Bytes(), &other)
		if err != nil {
			panic(err)
		}
	}

	return 1
}

type complexType struct {
	I1 int16
	I2 int32
	I3 int64
	I4 int

	I5 *int16
	I6 *int32
	I7 *int64
	I8 *int

	U1 uint16
	U2 uint32
	U3 uint64
	U4 uint

	U5 *uint16
	U6 *uint32
	U7 *uint64
	U8 *uint

	F1 float32
	F2 float64
	F3 *float32
	F4 *float64

	B1 bool
	B2 *bool

	S1 string
	S2 *string
	S3 []byte
	S4 *[]byte

	R1 *complexType
	R2 map[string]*complexType

	A []complexAttr
}

type complexAttr struct {
	I1 int16 `yson:",attr"`
	I2 int32 `yson:",attr"`
	I3 int64 `yson:",attr"`
	I4 int   `yson:",attr"`

	I5 *int16 `yson:",attr"`
	I6 *int32 `yson:",attr"`
	I7 *int64 `yson:",attr"`
	I8 *int   `yson:",attr"`

	U1 uint16 `yson:",attr"`
	U2 uint32 `yson:",attr"`
	U3 uint64 `yson:",attr"`
	U4 uint   `yson:",attr"`

	U5 *uint16 `yson:",attr"`
	U6 *uint32 `yson:",attr"`
	U7 *uint64 `yson:",attr"`
	U8 *uint   `yson:",attr"`

	F1 float32  `yson:",attr"`
	F2 float64  `yson:",attr"`
	F3 *float32 `yson:",attr"`
	F4 *float64 `yson:",attr"`

	B1 bool  `yson:",attr"`
	B2 *bool `yson:",attr"`

	S1 string  `yson:",attr"`
	S2 *string `yson:",attr"`
	S3 []byte  `yson:",attr"`
	S4 *[]byte `yson:",attr"`

	R1 complexType `yson:",value"`
}

func Marshal(data []byte) int {
	var val complexAttr
	err := yson.Unmarshal(data, &val)

	if err != nil {
		return 0
	}

	for _, format := range []yson.Format{yson.FormatPretty, yson.FormatText, yson.FormatBinary} {
		var buf bytes.Buffer
		w := yson.NewWriterFormat(&buf, format)
		enc := yson.NewEncoderWriter(w)

		err = enc.Encode(val)
		if err != nil {
			panic(err)
		}

		var other complexAttr
		err = yson.Unmarshal(buf.Bytes(), &other)
		if err != nil {
			panic(err)
		}
	}

	return 1
}
