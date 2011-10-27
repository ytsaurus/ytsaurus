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
        //const auto& impl = GetImpl<TFileNode>(request);
        BuildYsonFluently(request.Consumer)
            .Scalar(-1);

    }

    static void GetChunkId(const TGetRequest& request)
    {
        const auto& impl = GetImpl<TFileNode>(request);
        BuildYsonFluently(request.Consumer)
            .Scalar(impl.GetChunkId().ToString());
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

IAttributeProvider* TFileNodeProxy::GetAttributeProvider()
{
    return TFileNodeAttributeProvider::Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

