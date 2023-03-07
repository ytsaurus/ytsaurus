#include "shard_proxy.h"
#include "shard.h"
#include "node.h"

#include <yt/core/ytree/fluent.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/server/master/object_server/object_detail.h>

#include <yt/server/master/security_server/account.h>

#include <yt/server/master/cell_master/bootstrap.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

class TCypressShardProxy
    : public TNonversionedObjectProxyBase<TCypressShard>
{
public:
    TCypressShardProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TCypressShard* action)
        : TBase(bootstrap, metadata, action)
    { }

private:
    typedef TNonversionedObjectProxyBase<TCypressShard> TBase;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override
    {
        const auto* shard = GetThisImpl();

        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::RootNodeId)
            .SetPresent(shard->GetRoot() != nullptr));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::AccountStatistics)
            .SetOpaque(true));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::TotalAccountStatistics)
            .SetOpaque(true));
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::Name)
            .SetWritable(true));

        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* shard = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::RootNodeId:
                if (shard->GetRoot()) {
                    BuildYsonFluently(consumer)
                        .Value(shard->GetRoot()->GetId());
                    return true;
                }
                break;

            case EInternedAttributeKey::AccountStatistics:
                BuildYsonFluently(consumer)
                    .DoMapFor(shard->AccountStatistics(), [=] (auto fluent, const auto& accountAndStatistics) {
                            fluent
                                .Item(accountAndStatistics.first->GetName())
                                .Value(accountAndStatistics.second);
                        });
                return true;

            case EInternedAttributeKey::TotalAccountStatistics:
                BuildYsonFluently(consumer)
                    .Value(shard->ComputeTotalAccountStatistics());
                return true;

            case EInternedAttributeKey::Name:
                BuildYsonFluently(consumer)
                    .Value(shard->GetName());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(
        TInternedAttributeKey key,
        const TYsonString& value) override
    {
        auto* shard = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Name:
                shard->SetName(ConvertTo<TString>(value));
                return true;
        }

        return false;
    }
};


IObjectProxyPtr CreateCypressShardProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TCypressShard* shard)
{
    return New<TCypressShardProxy>(bootstrap, metadata, shard);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer

