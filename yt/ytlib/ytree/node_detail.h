#pragma once

#include "common.h"
#include "ytree.h"
#include "ypath_service.h"
#include "tree_builder.h"
#include "yson_reader.h"
#include "ypath_rpc.pb.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TNodeBase
    : public virtual INode
    , public virtual IYPathService
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

    virtual void Invoke(NRpc::IServiceContext* context);
    virtual TNavigateResult Navigate(TYPath path, bool mustExist);

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
    
    virtual void DoInvoke(NRpc::IServiceContext* context);
    virtual TNavigateResult NavigateRecursive(TYPath path, bool mustExist);

    RPC_SERVICE_METHOD_DECL(NProto, Get);
    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGet::TPtr context);
    virtual void GetRecursive(TYPath path, TReqGet* request, TRspGet* response, TCtxGet::TPtr context);

    RPC_SERVICE_METHOD_DECL(NProto, Set);
    virtual void SetSelf(TReqSet* request, TRspSet* response, TCtxSet::TPtr context);
    virtual void SetRecursive(TYPath path, TReqSet* request, TRspSet* response, TCtxSet::TPtr context);

    RPC_SERVICE_METHOD_DECL(NProto, Remove);
    virtual void RemoveSelf(TReqRemove* request, TRspRemove* response, TCtxRemove::TPtr context);
    virtual void RemoveRecursive(TYPath path, TReqRemove* request, TRspRemove* response, TCtxRemove::TPtr context);

    virtual void ThrowNonEmptySuffixPath(TYPath path);

    virtual yvector<Stroka> GetVirtualAttributeNames();
    virtual bool GetVirtualAttribute(const Stroka& name, IYsonConsumer* consumer);

};

////////////////////////////////////////////////////////////////////////////////

class TMapNodeMixin
    : public virtual IMapNode
{
protected:
    bool DoInvoke(NRpc::IServiceContext* context);
    IYPathService::TNavigateResult NavigateRecursive(TYPath path, bool mustExist);
    void SetRecursive(TYPath path, const TYson& value, ITreeBuilder* builder);
    void ThrowNonEmptySuffixPath(TYPath path);

private:
    IYPathService::TNavigateResult GetYPathChild(TYPath path) const;

    RPC_SERVICE_METHOD_DECL(NProto, List);

};

////////////////////////////////////////////////////////////////////////////////

class TListNodeMixin
    : public virtual IListNode
{
protected:
    IYPathService::TNavigateResult NavigateRecursive(TYPath path, bool mustExist);
    void SetRecursive(TYPath path, const TYson& value, ITreeBuilder* builder);
    void ThrowNonEmptySuffixPath(TYPath path);

private:
    IYPathService::TNavigateResult GetYPathChild(TYPath path) const;
    IYPathService::TNavigateResult GetYPathChild(int index, TYPath tailPath) const;

    void CreateYPathChild(
        int beforeIndex,
        TYPath tailPath,
        const TYson& value,
        ITreeBuilder* builder);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

