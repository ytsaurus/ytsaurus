#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/ypath/public.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <util/generic/bitops.h>

#include <optional>
#include <variant>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TResolveCacheNode
    : public TRefCounted
{
    // These fields are either const or only mutated in automaton thread.
    TCypressNode* TrunkNode;
    const NYPath::TYPath Path;
    TResolveCacheNode* Parent = nullptr;

    // These fields are mutated in automaton thread and are read in any thread.
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock);
    THashMap<TString, TResolveCacheNodePtr>::iterator ParentKeyToChildIt;

    struct TLinkPayload
    {
        NYPath::TYPath TargetPath;
    };
    struct TPortalEntrancePayload
    {
        TNodeId PortalExitId;
    };
    struct TMapPayload
    {
        THashMap<TString, TResolveCacheNodePtr> KeyToChild;
    };
    using TPayload = std::variant<
        TLinkPayload,
        TPortalEntrancePayload,
        TMapPayload
    >;
    TPayload Payload;

    TResolveCacheNode(TCypressNode* node, const NYPath::TYPath& path, TPayload payload);

    TNodeId GetId() const;
};

DEFINE_REFCOUNTED_TYPE(TResolveCacheNode)

////////////////////////////////////////////////////////////////////////////////

class TResolveCache
    : public TRefCounted
{
public:
    TResolveCache(
        TNodeId rootNodeId,
        bool primaryMaster);

    struct TResolveResult
    {
        TNodeId RemoteNodeId;
        NYPath::TYPath UnresolvedPathSuffix;
    };
    std::optional<TResolveResult> TryResolve(const NYPath::TYPath& path);

    TResolveCacheNodePtr FindNode(TNodeId nodeId);
    TResolveCacheNodePtr TryInsertNode(
        TCypressNode* trunkNode,
        const NYPath::TYPath& path);
    void AddNodeChild(
        const TResolveCacheNodePtr& parentNode,
        const TResolveCacheNodePtr& childNode,
        const TString& key);
    void InvalidateNode(TCypressNode* node);
    void Clear();

private:
    const TNodeId RootNodeId_;
    const bool PrimaryMaster_;

    struct TShard
    {
        THashMap<TNodeId, TResolveCacheNodePtr> IdToNode;

        //! Protects #IdToNode_ and #ResolveCacheNode_ for nodes.
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock);
    };

    static constexpr int ShardCount = 256;
    static_assert(IsPowerOf2(ShardCount), "Number of shards must be a power of two");

    std::array<TShard, ShardCount> Shards_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    TShard* GetShard(TNodeId nodeId);
    TShard* GetShard(TCypressNode* trunkNode);

    static TResolveCacheNode::TPayload MakePayload(TCypressNode* trunkNode);

    void ResetNode(TCypressNode* trunkNode);
};

DEFINE_REFCOUNTED_TYPE(TResolveCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
