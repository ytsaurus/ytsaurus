package jsonschema_test

import (
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

func TestPowerOfExt(t *testing.T) {
	t.Run("invalidSchema", func(t *testing.T) {
		c := jsonschema.NewCompiler()
		c.RegisterExtension("powerOf", powerOfMeta, powerOfCompiler{})
		if err := c.AddResource("test.json", strings.NewReader(`{"powerOf": "hello"}`)); err != nil {
			t.Fatal(err)
		}
		_, err := c.Compile("test.json")
		if err == nil {
			t.Fatal("error expected")
		}
		t.Log(err)
	})
	t.Run("validSchema", func(t *testing.T) {
		c := jsonschema.NewCompiler()
		c.RegisterExtension("powerOf", powerOfMeta, powerOfCompiler{})
		if err := c.AddResource("test.json", strings.NewReader(`{"powerOf": 10}`)); err != nil {
			t.Fatal(err)
		}
		sch, err := c.Compile("test.json")
		if err != nil {
			t.Fatal(err)
		}
		t.Run("validInstance", func(t *testing.T) {
			if err := sch.Validate(100); err != nil {
				t.Fatal(err)
			}
		})
		t.Run("invalidInstance", func(t *testing.T) {
			if err := sch.Validate(111); err == nil {
				t.Fatal("validation must fail")
			} else {
				t.Logf("%#v", err)
				if !strings.Contains(err.(*jsonschema.ValidationError).GoString(), "111 not powerOf 10") {
					t.Fatal("validation error expected to contain powerOf message")
				}
			}
		})
	})
}
