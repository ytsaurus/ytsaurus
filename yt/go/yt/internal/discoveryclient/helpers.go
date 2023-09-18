package discoveryclient

import (
	"go.ytsaurus.tech/yt/go/proto/client/discovery_client"
	"go.ytsaurus.tech/yt/go/proto/core/ytree"
	"go.ytsaurus.tech/yt/go/yt"
)

func convertListMembersOptions(opts *yt.ListMembersOptions) *discovery_client.TListMembersOptions {
	if opts == nil {
		return nil
	}
	return &discovery_client.TListMembersOptions{
		Limit:         opts.Limit,
		AttributeKeys: opts.AttributeKeys,
	}
}

func makeAttributes(attrs *ytree.TAttributeDictionary) []*yt.Attribute {
	if attrs == nil {
		return nil
	}
	ret := make([]*yt.Attribute, 0, len(attrs.Attributes))
	for _, attr := range attrs.Attributes {
		ret = append(ret, &yt.Attribute{
			Key:   *attr.Key,
			Value: attr.Value,
		})
	}
	return ret
}

func makeMembers(members []*discovery_client.TMemberInfo) []*yt.MemberInfo {
	ret := make([]*yt.MemberInfo, 0, len(members))
	for _, member := range members {
		ret = append(ret, &yt.MemberInfo{
			ID:         *member.Id,
			Priority:   *member.Priority,
			Revision:   *member.Revision,
			Attributes: makeAttributes(member.Attributes),
		})
	}
	return ret
}

func convertMemberInfo(memberInfo *yt.MemberInfo) *discovery_client.TMemberInfo {
	attrDict := &ytree.TAttributeDictionary{}
	if memberInfo.Attributes != nil {
		attrs := make([]*ytree.TAttribute, 0, len(memberInfo.Attributes))
		for _, attr := range memberInfo.Attributes {
			attrs = append(attrs, &ytree.TAttribute{
				Key:   &attr.Key,
				Value: attr.Value,
			})
		}
		attrDict.Attributes = attrs
	}
	return &discovery_client.TMemberInfo{
		Id:         &memberInfo.ID,
		Priority:   &memberInfo.Priority,
		Revision:   &memberInfo.Revision,
		Attributes: attrDict,
	}
}

func makeMeta(meta *discovery_client.TGroupMeta) *yt.GroupMeta {
	return &yt.GroupMeta{MemberCount: *meta.MemberCount}
}
