#include "stdafx.h"
#include "file_node_proxy.h"

#include "../cypress/attribute.h"
#include "../ytree/fluent.h"

namespace NYT {
namespace NFileServer {

using namespace NCypress;

////////////////////////////////////////////////////////////////////////////////

class TFileNodeAttributeProvider
    : public TCypressNodeAttributeProvider
{
public:
    static IAttributeProvider* Get()
    {
        return Singleton<TFileNodeAttributeProvider>();
    }

    TFileNodeAttributeProvider()
    {
        RegisterGetter("size", FromMethod(&TThis::GetSize));
        RegisterGetter("chunk_id", FromMethod(&TThis::GetChunkId));
    }

private:
    typedef TFileNodeAttributeProvider TThis;

    static void GetSize(const TGetRequest& request)
    {
        auto* typedProxy = dynamic_cast<TFileNodeProxy*>(~request.Proxy);
        YASSERT(typedProxy != NULL);
        BuildYsonFluently(request.Consumer)
            .Scalar(-1);

    }

    static void GetChunkId(const TGetRequest& request)
    {
        auto* typedProxy = dynamic_cast<TFileNodeProxy*>(~request.Proxy);
        YASSERT(typedProxy != NULL);
        BuildYsonFluently(request.Consumer)
            .Scalar(typedProxy->GetChunkId().ToString());
    }
};

////////////////////////////////////////////////////////////////////////////////

TFileNodeProxy::TFileNodeProxy(
    TCypressManager::TPtr cypressManager,
    const TTransactionId& transactionId,
    const TNodeId& nodeId)
    : TCypressNodeProxyBase<IEntityNode, TFileNode>(
        cypressManager,
        transactionId,
        nodeId)
{ }

NYT::NYTree::ENodeType TFileNodeProxy::GetType() const
{
    return ENodeType::Entity;
}

Stroka TFileNodeProxy::GetTypeName() const
{
    return FileTypeName;
}

TChunkId TFileNodeProxy::GetChunkId() const
{
    return GetTypedImpl().GetChunkId();
}

void TFileNodeProxy::SetChunkId(const TChunkId& chunkId)
{
    EnsureModifiable();
    GetTypedImplForUpdate().SetChunkId(chunkId);
}

IAttributeProvider* TFileNodeProxy::GetAttributeProvider()
{
    return TFileNodeAttributeProvider::Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

