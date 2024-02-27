package jsonschema_test

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

var powerOfMeta = jsonschema.MustCompileString("powerOf.json", `{
	"properties" : {
		"powerOf": {
			"type": "integer",
			"exclusiveMinimum": 0
		}
	}
}`)

type powerOfCompiler struct{}

func (powerOfCompiler) Compile(ctx jsonschema.CompilerContext, m map[string]interface{}) (jsonschema.ExtSchema, error) {
	if pow, ok := m["powerOf"]; ok {
		n, err := pow.(json.Number).Int64()
		return powerOfSchema(n), err
	}

	// nothing to compile, return nil
	return nil, nil
}

type powerOfSchema int64

func (s powerOfSchema) Validate(ctx jsonschema.ValidationContext, v interface{}) error {
	switch v.(type) {
	case json.Number, float32, float64, int, int8, int32, int64, uint, uint8, uint32, uint64:
		pow := int64(s)
		n, _ := strconv.ParseInt(fmt.Sprint(v), 10, 64)
		for n%pow == 0 {
			n = n / pow
		}
		if n != 1 {
			return ctx.Error("powerOf", "%v not powerOf %v", v, pow)
		}
		return nil
	default:
		return nil
	}
}

func Example_extension() {
	c := jsonschema.NewCompiler()
	c.RegisterExtension("powerOf", powerOfMeta, powerOfCompiler{})

	schema := `{"powerOf": 10}`
	instance := `100`

	if err := c.AddResource("schema.json", strings.NewReader(schema)); err != nil {
		log.Fatal(err)
	}
	sch, err := c.Compile("schema.json")
	if err != nil {
		log.Fatalf("%#v", err)
	}

	var v interface{}
	if err := json.Unmarshal([]byte(instance), &v); err != nil {
		log.Fatal(err)
	}

	if err = sch.Validate(v); err != nil {
		log.Fatalf("%#v", err)
	}
	// Output:
}
