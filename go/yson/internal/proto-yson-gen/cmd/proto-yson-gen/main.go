package main

import (
	"io/ioutil"
	"os"

	"github.com/golang/protobuf/proto"

	"a.yandex-team.ru/yp/go/yson/internal/proto-yson-gen/generator"
)

func main() {
	g := generator.New()
	input, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		g.Error(err, "reading input")
	}

	if err := proto.Unmarshal(input, g.Request); err != nil {
		g.Error(err, "parsing input proto")
	}

	if len(g.Request.FileToGenerate) == 0 {
		g.Fail("no files to generate")
	}

	g.CommandLineParameters(g.Request.GetParameter())

	g.WrapTypes()
	g.SetPackageNames()
	g.BuildTypeNameMap()
	g.GenerateAllFiles()

	data, err := proto.Marshal(g.Response)
	if err != nil {
		g.Error(err, "failed to marshal output proto")
	}

	_, err = os.Stdout.Write(data)
	if err != nil {
		g.Error(err, "failed to write output proto")
	}
}
