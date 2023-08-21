package yson_test

import (
	"fmt"
	"strings"

	"go.ytsaurus.tech/yt/go/yson"
)

type Inner struct {
	G bool `yson:"g,attr"`
}

type anonymous struct {
	F *int `yson:"f,attr"`
	*Inner
}

type Anonymous struct {
	F *int `yson:"f,attr"`
	*Inner
}

type MyStruct struct {
	Value  string  `yson:",value"`
	Attr   int64   `yson:"attr,attr"`
	NoName float64 `yson:",attr"`
	NoAttr string  `yson:"no_attr"`
	NoTag  string
	Inner  *Inner `yson:"inner,attr"`
	anonymous
	Anonymous `yson:"a,attr"`
}

var Attrs = yson.MustInferAttrs(&MyStruct{})

func ExampleMustInferAttrs() {
	fmt.Println(strings.Join(Attrs, ","))

	// Output:
	// attr,NoName,inner,f,g,a
}
