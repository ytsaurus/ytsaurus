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

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %+v\n", err)
	os.Exit(1)
}

var (
	flagProxy = flag.String("proxy", "", "cluster address")
)

func main() {
	flag.Parse()

	yc, err := ythttp.NewClientCli(*flagProxy)
	if err != nil {
		fatal(err)
	}

	ctx := context.Background()

	var attrs struct {
		Type         yt.NodeType `yson:"type"`
		CreationTime yson.Time   `yson:"creation_time"`
		Account      string      `yson:"account"`
	}

	if err = yc.GetNode(ctx, ypath.Path("//@"), &attrs, nil); err != nil {
		fatal(err)
	}

	fmt.Printf("cluster was created at %v\n", time.Time(attrs.CreationTime))
}
