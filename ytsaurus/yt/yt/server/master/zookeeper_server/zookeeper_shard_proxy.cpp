#include "zookeeper_shard_proxy.h"

#include "zookeeper_shard.h"

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NZookeeperServer {

using namespace NObjectServer;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TZookeeperShardProxy
    : public TNonversionedObjectProxyBase<TZookeeperShard>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TZookeeperShard>;

    void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Name)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::RootPath)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::CellTag)
            .SetReplicated(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* shard = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Name:
                BuildYsonFluently(consumer)
                    .Value(shard->GetName());
                return true;

            case EInternedAttributeKey::RootPath:
                BuildYsonFluently(consumer)
                    .Value(shard->GetRootPath());
                return true;

            case EInternedAttributeKey::CellTag:
                BuildYsonFluently(consumer)
                    .Value(shard->GetCellTag());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    void ValidateRemoval() override
    {
        ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateZookeeperShardProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TZookeeperShard* shard)
{
    return New<TZookeeperShardProxy>(bootstrap, metadata, shard);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperServer
