package schema_test

import (
	"fmt"

	"go.ytsaurus.tech/yt/go/schema"
)

type MyStruct struct {
	IntColumn    int
	AnyValue     interface{}
	Optional     *int
	StringColumn string `yson:"custom_column_name"`
	SkipMe       int    `yson:"-"`
}

var MySchema = schema.MustInfer(&MyStruct{})

func ExampleInfer() {
	for _, column := range MySchema.Columns {
		var kind string
		if column.Required {
			kind = "required"
		} else {
			kind = "optional"
		}

		fmt.Printf("%q is %s column of type %s\n", column.Name, kind, column.Type)
	}

	// Output:
	// "IntColumn" is required column of type int64
	// "AnyValue" is optional column of type any
	// "Optional" is optional column of type int64
	// "custom_column_name" is required column of type utf8
}
