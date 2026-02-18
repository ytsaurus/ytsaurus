#include "misc.h"

#include <util/system/env.h>
#include <yt/yt/library/auth/auth.h>

#include <util/generic/string.h>

TString LoadResourceUsageToken()
{
    auto ytResourceUsageToken = GetEnv("YT_RESOURCE_USAGE_TOKEN");
    Y_ABORT_IF(ytResourceUsageToken.empty());
    NYT::NAuth::ValidateToken(ytResourceUsageToken);
    return ytResourceUsageToken;
}

void MergeIntNodeWith(NYT::TNode& to, const NYT::TNode& from)
{
    if (!to.IsMap()) {
        Y_ABORT_IF(from.IsMap());
        to.As<i64>() += from.As<i64>();
        return;
    }

    Y_ABORT_IF(!from.IsMap());

    auto& to_map = to.AsMap();
    auto& from_map = from.AsMap();

    for (const auto& [from_key, from_node] : from_map) {
        auto to_it = to_map.find(from_key);
        if (to_it == to_map.end()) {
            to_map[from_key] = from_node;
        } else {
            MergeIntNodeWith(to_it->second, from_node);
        }
    }
}

i64 GetInt64FromTNode(const NYT::TNode& node, const TStringBuf key)
{
    const auto& nodeMap = node.AsMap();
    auto it = nodeMap.find(key);
    if (it != nodeMap.end()) {
        return it->second.AsInt64();
    }
    return 0;
}


TVersionedResourceUsage::TVersionedResourceUsage()
    : TransactionTitle("")
    , VersionedResourceUsageNode(NYT::TNode::CreateMap())
    , DiskSpacePerMedium(NYT::TNode::CreateMap())
    , IsOriginal(false)
{ }

TVersionedResourceUsage::TVersionedResourceUsage(TString title, NYT::TNode vrun, NYT::TNode dspm, bool IsOriginal)
    : TransactionTitle(title)
    , VersionedResourceUsageNode(vrun)
    , DiskSpacePerMedium(dspm)
    , IsOriginal(IsOriginal)
{ }

void TVersionedResourceUsage::MergeWith(const TVersionedResourceUsage& from)
{
    if (this->TransactionTitle == "") {
        this->TransactionTitle = from.TransactionTitle;
    }
    MergeIntNodeWith(this->VersionedResourceUsageNode, from.VersionedResourceUsageNode);
    MergeIntNodeWith(this->DiskSpacePerMedium, from.DiskSpacePerMedium);
    IsOriginal |= IsOriginal;
}

NYT::TNode TVersionedResourceUsage::ToNode() const
{
    NYT::TNode result = NYT::TNode::CreateMap();
    result["transaction_title"] = TransactionTitle;
    result["versioned_resource_usage"] = VersionedResourceUsageNode;
    result["per_medium"] = DiskSpacePerMedium;
    return result;
}

bool GreaterByDiskSpace(const std::pair<TString, TVersionedResourceUsage>& l, const std::pair<TString, TVersionedResourceUsage>& r)
{
    auto getDiskSpace = [] (const auto& x) {
        return GetInt64FromTNode(x.second.DiskSpacePerMedium, "regular_disk_space") + GetInt64FromTNode(x.second.DiskSpacePerMedium, "erasure_disk_space");
    };
    return getDiskSpace(l) > getDiskSpace(r);
}

bool GreaterByNodeCount(const std::pair<TString, TVersionedResourceUsage>& l, const std::pair<TString, TVersionedResourceUsage>& r)
{
    auto getNodeCount = [] (const auto& x) {
        return GetInt64FromTNode(x.second.VersionedResourceUsageNode, "node_count");
    };
    return getNodeCount(l) > getNodeCount(r);
}

bool GreaterByTabletCount(const std::pair<TString, TVersionedResourceUsage>& l, const std::pair<TString, TVersionedResourceUsage>& r)
{
    auto getTabletCount = [] (const auto& x) {
        return GetInt64FromTNode(x.second.VersionedResourceUsageNode, "tablet_count");
    };
    return getTabletCount(l) > getTabletCount(r);
}

bool GreaterByTabletStaticMemory(const std::pair<TString, TVersionedResourceUsage>& l, const std::pair<TString, TVersionedResourceUsage>& r)
{
    auto getTabletStaticMemory = [] (const auto& x) {
        return GetInt64FromTNode(x.second.VersionedResourceUsageNode, "tablet_static_memory");
    };
    return getTabletStaticMemory(l) > getTabletStaticMemory(r);
}

bool GreaterByMasterMemory(const std::pair<TString, TVersionedResourceUsage>& l, const std::pair<TString, TVersionedResourceUsage>& r)
{
    auto getMasterMemory = [] (const auto& x) {
        return GetInt64FromTNode(x.second.VersionedResourceUsageNode, "master_memory");
    };
    return getMasterMemory(l) > getMasterMemory(r);
}

NYT::TNode VersionedResourceUsageTop(const TVersionedResourceUsageMap& vrum, ui64 topN)
{
    NYT::TNode result = NYT::TNode::CreateMap();
    std::vector<std::pair<TString, TVersionedResourceUsage>> vrum_vec(vrum.begin(), vrum.end());
    auto topEnd = std::next(vrum_vec.begin(), topN);
    auto addTop = [&] (auto cmp) {
        std::nth_element(vrum_vec.begin(), topEnd, vrum_vec.end(), cmp);
        for (auto it = vrum_vec.begin(); it != topEnd; ++it) {
            result.AsMap()[it->first] = it->second.ToNode();
        }
    };
    addTop(GreaterByDiskSpace);
    addTop(GreaterByNodeCount);
    addTop(GreaterByTabletCount);
    addTop(GreaterByTabletStaticMemory);
    addTop(GreaterByMasterMemory);
    return result;
}

