package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/bus"
	"go.ytsaurus.tech/yt/go/proto/client/api/rpc_proxy"
)

var (
	flagAddress = flag.String("address", "man2-4299-b52.hume.yt.gencfg-c.yandex.net:9013", "Address of YT rpc proxy")
)

func testBus() error {
	ctx := context.Background()
	conn := bus.NewClient(ctx, *flagAddress)
	defer conn.Close()

	var req rpc_proxy.TReqDiscoverProxies
	var rsp rpc_proxy.TRspDiscoverProxies

	if err := conn.Send(ctx, "DiscoveryService", "DiscoverProxies", &req, &rsp); err != nil {
		return err
	}

	fmt.Printf("proxies = %s\n", rsp.String())
	return nil
}

func main() {
	flag.Parse()

	if err := testBus(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "err: %+v\n", err)
	}
}
