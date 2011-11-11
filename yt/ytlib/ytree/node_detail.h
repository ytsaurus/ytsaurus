#pragma once

#include "common.h"
#include "ytree.h"
#include "ypath.h"
#include "tree_builder.h"
#include "yson_reader.h"
#include "ytree_rpc.pb.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TNodeBase
    : public virtual IYPathService
    , public virtual IYPathService2
    , public virtual INode
{
public:
    typedef TIntrusivePtr<TNodeBase> TPtr;

#define IMPLEMENT_AS_METHODS(name) \
    virtual TIntrusivePtr<I##name##Node> As##name() \
    { \
        ythrow yexception() << Sprintf("Invalid node type (Expected: %s, Actual: %s)", \
            #name, \
            ~GetType().ToString()); \
    } \
    \
    virtual TIntrusivePtr<const I##name##Node> As##name() const \
    { \
        ythrow yexception() << Sprintf("Invalid node type (Expected: %s, Actual: %s)", \
            #name, \
            ~GetType().ToString()); \
    }

    IMPLEMENT_AS_METHODS(Entity)
    IMPLEMENT_AS_METHODS(Composite)

    IMPLEMENT_AS_METHODS(String)
    IMPLEMENT_AS_METHODS(Int64)
    IMPLEMENT_AS_METHODS(Double)
    IMPLEMENT_AS_METHODS(List)
    IMPLEMENT_AS_METHODS(Map)

#undef IMPLEMENT_AS_METHODS

    virtual TNavigateResult2 Navigate2(TYPath path);

    virtual TGetResult Get(TYPath path, IYsonConsumer* consumer);

    virtual TSetResult Set(TYPath path, TYsonProducer::TPtr producer);

    virtual TRemoveResult Remove(TYPath path);

    virtual TLockResult Lock(TYPath path);

protected:
    template <class TNode>
    static void DoSet(
        TNode* node,
        const Stroka& value,
        ITreeBuilder* builder)
    {
        TStringInput stream(value);
        SetNodeFromProducer(node, ~TYsonReader::GetProducer(&stream), builder);
    }
    
    virtual void Invoke2(NRpc::IServiceContext* context);
    virtual TNavigateResult2 NavigateRecursive2(TYPath path);

    RPC_SERVICE_METHOD_DECL(NProto, Get2);
    virtual void GetSelf2(TReqGet2* request, TRspGet2* response, TCtxGet2::TPtr context);
    virtual void GetRecursive2(TYPath path, TReqGet2* request, TRspGet2* response, TCtxGet2::TPtr context);

    RPC_SERVICE_METHOD_DECL(NProto, Set2);
    virtual void SetSelf2(TReqSet2* request, TRspSet2* response, TCtxSet2::TPtr context);
    virtual void SetRecursive2(TYPath path, TReqSet2* request, TRspSet2* response, TCtxSet2::TPtr context);

    RPC_SERVICE_METHOD_DECL(NProto, Remove2);
    virtual void RemoveSelf2(TReqRemove2* request, TRspRemove2* response, TCtxRemove2::TPtr context);
    virtual void RemoveRecursive2(TYPath path, TReqRemove2* request, TRspRemove2* response, TCtxRemove2::TPtr context);



    virtual TNavigateResult Navigate(TYPath path);
    virtual TNavigateResult NavigateRecursive(TYPath path);

    virtual TRemoveResult RemoveSelf();
    virtual TRemoveResult RemoveRecursive(TYPath path);

    virtual TGetResult GetSelf(IYsonConsumer* consumer);
    virtual TGetResult GetRecursive(TYPath path, IYsonConsumer* consumer);

    virtual TSetResult SetSelf(TYsonProducer::TPtr producer);
    virtual TSetResult SetRecursive(TYPath path, TYsonProducer::TPtr producer);

    virtual TLockResult LockSelf();
    virtual TLockResult LockRecursive(TYPath path);
    
    virtual yvector<Stroka> GetVirtualAttributeNames();
    virtual bool GetVirtualAttribute(const Stroka& name, IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeMixin
    : public virtual IMapNode
{
protected:
    IYPathService2::TNavigateResult2 NavigateRecursive2(TYPath path);
    void SetRecursive2(TYPath path, const TYson& value, ITreeBuilder* builder);


    IYPathService::TNavigateResult NavigateRecursive(TYPath path);

    IYPathService::TSetResult SetRecursive(
        TYPath path,
        TYsonProducer* producer,
        ITreeBuilder* builder);
};

////////////////////////////////////////////////////////////////////////////////

class TListNodeMixin
    : public virtual IListNode
{
protected:
    IYPathService2::TNavigateResult2 NavigateRecursive2(TYPath path);
    void SetRecursive2(TYPath path, const TYson& value, ITreeBuilder* builder);


    IYPathService::TNavigateResult NavigateRecursive(TYPath path);

    IYPathService::TSetResult SetRecursive(
        TYPath path,
        TYsonProducer* producer,
        ITreeBuilder* builder);

    IYPathService::TNavigateResult GetYPathChild(
        int index,
        TYPath tailPath) const;
    IYPathService2::TNavigateResult2 GetYPathChild2(
        int index,
        TYPath tailPath) const;

    IYPathService::TSetResult CreateYPathChild(
        int beforeIndex,
        TYPath tailPath,
        TYsonProducer* producer,
        ITreeBuilder* builder);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

