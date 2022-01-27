#include "chaos_replicated_table_node_proxy.h"
#include "chaos_replicated_table_node.h"

#include <yt/yt/server/master/cypress_server/node_proxy_detail.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/chaos_client/replication_card.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/rpc/authentication_identity.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChaosServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NCypressServer;
using namespace NCellMaster;
using namespace NChaosClient;
using namespace NApi;
using namespace NApi::NNative;

////////////////////////////////////////////////////////////////////////////////

class TChaosReplicatedTableNodeProxy
    : public TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TChaosReplicatedTableNode>
{
public:
    YTREE_NODE_TYPE_OVERRIDES_WITH_CHECK(Entity)

public:
    using TCypressNodeProxyBase::TCypressNodeProxyBase;

private:
    using TBase = TCypressNodeProxyBase<TNontemplateCypressNodeProxyBase, IEntityNode, TChaosReplicatedTableNode>;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::ReplicationCardId);
        descriptors->push_back(EInternedAttributeKey::OwnsReplicationCard);
        descriptors->push_back(EInternedAttributeKey::Era);
        descriptors->push_back(EInternedAttributeKey::CoordinatorCellIds);
        descriptors->push_back(EInternedAttributeKey::Replicas);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* impl = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::ReplicationCardId:
                BuildYsonFluently(consumer)
                    .Value(impl->GetReplicationCardId());
                return true;

            case EInternedAttributeKey::OwnsReplicationCard:
                BuildYsonFluently(consumer)
                    .Value(impl->GetOwnsReplicationCard());
                return true;

            default:
                break;
        }

        return TCypressNodeProxyBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        switch (key) {
            case EInternedAttributeKey::OwnsReplicationCard: {
                ValidateNoTransaction();
                auto* lockedImpl = LockThisImpl();
                lockedImpl->SetOwnsReplicationCard(ConvertTo<bool>(value));
                return true;
            }

            default:
                break;
        }

        return TCypressNodeProxyBase::SetBuiltinAttribute(key, value);
    }

    TFuture<TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override
    {
        switch (key) {
            case EInternedAttributeKey::Era:
                return GetReplicationCard()
                    .Apply(BIND([] (const TReplicationCardPtr& card) {
                        return BuildYsonStringFluently()
                            .Value(card->Era);
                    }));

            case EInternedAttributeKey::CoordinatorCellIds:
                return GetReplicationCard({.IncludeCoordinators = true})
                    .Apply(BIND([] (const TReplicationCardPtr& card) {
                        return BuildYsonStringFluently()
                            .Value(card->CoordinatorCellIds);
                    }));

            case EInternedAttributeKey::Replicas:
                return GetReplicationCard()
                    .Apply(BIND([] (const TReplicationCardPtr& card) {
                        return BuildYsonStringFluently()
                            .Value(card->Replicas);
                    }));

            default:
                break;
        }

        return TCypressNodeProxyBase::GetBuiltinAttributeAsync(key);
    }

    TFuture<TReplicationCardPtr> GetReplicationCard(const TReplicationCardFetchOptions& options = {})
    {
        const auto& connection = Bootstrap_->GetClusterConnection();
        auto clientOptions = TClientOptions::FromAuthenticationIdentity(NRpc::GetCurrentAuthenticationIdentity());
        auto client = connection->CreateClient(clientOptions);
        const auto* impl = GetThisImpl();
        TGetReplicationCardOptions getCardOptions;
        static_cast<TReplicationCardFetchOptions&>(getCardOptions) = options;
        getCardOptions.BypassCache = true;
        return client->GetReplicationCard(impl->GetReplicationCardId(), getCardOptions)
            .Apply(BIND([client] (const TReplicationCardPtr& card) {
                return card;
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

ICypressNodeProxyPtr CreateChaosReplicatedTableNodeProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TTransaction* transaction,
    TChaosReplicatedTableNode* trunkNode)
{
    return New<TChaosReplicatedTableNodeProxy>(
        bootstrap,
        metadata,
        transaction,
        trunkNode);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
