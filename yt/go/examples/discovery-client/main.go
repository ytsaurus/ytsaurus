package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.ytsaurus.tech/library/go/ptr"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yt/ytdiscovery"
)

type addr struct {
	host string
	port uint64
}

func getAddrFromAttrs(attrs []*yt.Attribute) addr {
	var res addr
	for _, attr := range attrs {
		if attr.Key == "host" {
			_ = yson.Unmarshal(attr.Value, &res.host)
		} else if attr.Key == "monitoring_port" {
			_ = yson.Unmarshal(attr.Value, &res.port)
		}
	}
	return res
}

func main() {
	c, err := ytdiscovery.NewStatic(&yt.DiscoveryConfig{
		DiscoveryServers: []string{
			"sas2-2058-discovery-hume.man-pre.yp-c.yandex.net:9020",
			"sas2-2059-discovery-hume.man-pre.yp-c.yandex.net:9020",
			"sas2-2060-discovery-hume.man-pre.yp-c.yandex.net:9020",
		},
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
	defer c.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*1)
	defer cancel()

	groupsPath := "/chyt"

	rsp, err := c.ListGroups(ctx, groupsPath, &yt.ListGroupsOptions{Limit: ptr.Int32(2)})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
	fmt.Println(rsp)

	groupID := rsp.GroupIDs[0]

	meta, err := c.GetGroupMeta(ctx, groupID, nil)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}
	fmt.Println(meta.MemberCount)

	members, err := c.ListMembers(ctx, groupID, &yt.ListMembersOptions{
		Limit:         ptr.Int32(10),
		AttributeKeys: []string{"host", "monitoring_port"},
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "error: %+v\n", err)
		os.Exit(1)
	}

	for i, member := range members {
		address := getAddrFromAttrs(member.Attributes)
		fmt.Printf("%v member:%v\n", i, address)
	}
}
