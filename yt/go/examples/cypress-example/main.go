package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"a.yandex-team.ru/yt/go/ypath"
	"a.yandex-team.ru/yt/go/yson"
	"a.yandex-team.ru/yt/go/yt"
	"a.yandex-team.ru/yt/go/yt/ythttp"
)

var (
	flagProxy = flag.String("proxy", "", "cluster address")
)

func Example() error {
	flag.Parse()

	yc, err := ythttp.NewClientCli(*flagProxy)
	if err != nil {
		return err
	}

	ctx := context.Background()

	var attrs struct {
		Type         yt.NodeType `yson:"type"`
		CreationTime yson.Time   `yson:"creation_time"`
		Account      string      `yson:"account"`
	}

	if err = yc.GetNode(ctx, ypath.Path("//@"), &attrs, nil); err != nil {
		return err
	}

	fmt.Printf("cluster was created at %v\n", time.Time(attrs.CreationTime))
	return nil
}

func main() {
	if err := Example(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
}
