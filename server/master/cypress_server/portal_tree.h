//#pragma once
//
//#include "public.h"
//
//#include <yt/core/concurrency/rw_spinlock.h>
//
//#include <yt/core/ypath/public.h>
//
//#include <optional>
//#include <variant>
//
//namespace NYT::NCypressServer {
//
//////////////////////////////////////////////////////////////////////////////////
//
//struct TPortalTreeNode
//    : public TIntrinsicRefCounted
//{
//    TNodeId NodeId;
//    std::atomic<bool> Invalid = {false};
//    NConcurrency::TReaderWriterSpinLock Lock;
//
//    struct TLinkPayload
//    {
//        NYPath::TYPath Path;
//    };
//    struct TPortalPayload
//    {
//        NObjectClient::TCellTag ExitCellTag;
//    };
//    struct TMapPayload
//    {
//        THashMap<TString, TPortalTreeNodePtr> KeyToChild;
//    };
//    std::variant<
//        TLinkPayload,
//        TPortalPayload,
//        TMapPayload
//    > Payload;
//};
//
//DEFINE_REFCOUNTED_TYPE(TPortalTreeNode)
//
//////////////////////////////////////////////////////////////////////////////////
//
//class TPortalTree
//    :  public TIntrinsicRefCounted
//{
//public:
//    struct TResolveResult
//    {
//        TNodeId PortalExitId;
//        TStringBuf ResolvedPrefix;
//    };
//    std::optional<TResolveResult> TryResolve(TNodeId rootId, const NYPath::TYPath& path);
//
//    void InvalidateNode(TNodeId nodeId);
//    void Prune();
//
//private:
//    NConcurrency::TReaderWriterSpinLock IdToNodeLock_;
//    THashMap<TNodeId, TPortalTreeNodePtr> IdToNode_;
//
//    TPortalTreeNodePtr FindNode(TNodeId nodeId);
//    static TNodeId ParseRootId(
//        TNodeId rootId,
//        NYPath::TTokenizer* tokenizer)
//
//};
//
//DEFINE_REFCOUNTED_TYPE(TPortalTree)
//
//////////////////////////////////////////////////////////////////////////////////
//
//} // namespace NYT::NCypressServer
