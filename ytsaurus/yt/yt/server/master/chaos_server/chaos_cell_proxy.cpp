#include "chaos_cell_proxy.h"

#include "alien_cluster_registry.h"
#include "chaos_cell.h"
#include "chaos_manager.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cell_server/cell_bundle.h>
#include <yt/yt/server/master/cell_server/cell_proxy_base.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NChaosServer {

using namespace NObjectServer;
using namespace NCellMaster;
using namespace NCellServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TChaosCellProxy
    : public TCellProxyBase
{
public:
    using TBase = TCellProxyBase;
    using TCellProxyBase::TCellProxyBase;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::AlienConfigVersions)
            .SetOpaque(true));
        descriptors->push_back(EInternedAttributeKey::DescriptorConfigVersion);

        TBase::ListSystemAttributes(descriptors);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* cell = GetThisImpl<TChaosCell>();
        const auto& alienClusterRegistry = Bootstrap_->GetChaosManager()->GetAlienClusterRegistry();

        switch (key) {
            case EInternedAttributeKey::AlienConfigVersions:
                BuildYsonFluently(consumer)
                    .DoMapFor(cell->AlienConfigVersions(), [&] (TFluentMap fluent, const auto& pair) {
                        fluent.Item(alienClusterRegistry->GetAlienClusterName(pair.first)).Value(pair.second);
                    });
                return true;

            case EInternedAttributeKey::DescriptorConfigVersion:
                BuildYsonFluently(consumer)
                    .Value(cell->GetDescriptorConfigVersion());
                return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateChaosCellProxy(
    TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TChaosCell* cell)
{
    return New<TChaosCellProxy>(bootstrap, metadata, cell);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
