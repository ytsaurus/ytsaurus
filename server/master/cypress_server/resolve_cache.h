#pragma once

#include "public.h"

#include <yt/core/concurrency/rw_spinlock.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/ypath/public.h>

#include <optional>
#include <variant>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

struct TResolveCacheNode
    : public TIntrinsicRefCounted
{
    // These fields are either const or only mutated in automaton thread.
    TCypressNode* TrunkNode;
    const NYPath::TYPath Path;
    TResolveCacheNode* Parent = nullptr;

    // These fields are mutated in automaton thread and are read in any thread.
    NConcurrency::TReaderWriterSpinLock Lock;
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
    :  public TIntrinsicRefCounted
{
public:
    explicit TResolveCache(TNodeId rootNodeId);

    struct TResolveResult
    {
        TNodeId PortalExitId;
        NYPath::TYPath UnresolvedPathSuffix;
    };
    std::optional<TResolveResult> TryResolve(const NYPath::TYPath& path);

    TResolveCacheNodePtr FindNode(TNodeId nodeId);
    TResolveCacheNodePtr InsertNode(
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

    NConcurrency::TReaderWriterSpinLock IdToNodeLock_;
    THashMap<TNodeId, TResolveCacheNodePtr> IdToNode_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);

    static TResolveCacheNode::TPayload MakePayload(TCypressNode* trunkNode);
    static void ResetNode(TCypressNode* trunkNode);
};

DEFINE_REFCOUNTED_TYPE(TResolveCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
