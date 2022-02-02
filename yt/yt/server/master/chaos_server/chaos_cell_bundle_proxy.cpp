#include "config.h"
#include "private.h"
#include "chaos_cell.h"
#include "chaos_cell_bundle.h"
#include "chaos_cell_bundle_proxy.h"
#include "chaos_manager.h"

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cell_server/cell_bundle_proxy.h>
#include <yt/yt/server/master/cell_server/tamed_cell_manager.h>

#include <yt/yt/server/master/node_tracker_server/node.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/client/table_client/public.h>

namespace NYT::NChaosServer {

using namespace NYTree;
using namespace NYson;
using namespace NTableClient;
using namespace NCellServer;
using namespace NTableServer;
using namespace NObjectServer;
using namespace NNodeTrackerServer;

////////////////////////////////////////////////////////////////////////////////

class TChaosCellBundleProxy
    : public TCellBundleProxy
{
public:
    using TCellBundleProxy::TCellBundleProxy;

private:
    using TBase = TCellBundleProxy;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        const auto* cellBundle = GetThisImpl<TChaosCellBundle>();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ChaosOptions)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MetadataCellId)
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true)
            .SetPresent(IsObjectAlive(cellBundle->GetMetadataCell())));

        TBase::ListSystemAttributes(descriptors);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* cellBundle = GetThisImpl<TChaosCellBundle>();

        switch (key) {
            case EInternedAttributeKey::ChaosOptions:
                BuildYsonFluently(consumer)
                    .Value(cellBundle->GetChaosOptions());
                return true;

            case EInternedAttributeKey::MetadataCellId:
                if (const auto* metadataCell = cellBundle->GetMetadataCell()) {
                    BuildYsonFluently(consumer)
                        .Value(metadataCell->GetId());
                    return true;
                } else {
                    return false;
                }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        auto* cellBundle = GetThisImpl<TChaosCellBundle>();

        switch (key) {
            case EInternedAttributeKey::MetadataCellId: {
                auto metadataCellId = ConvertTo<TChaosCellId>(value);
                const auto& cellManager = Bootstrap_->GetTamedCellManager();
                auto* metadataCell = cellManager->GetCellOrThrow(metadataCellId);
                if (metadataCell->GetCellBundle() != cellBundle) {
                    THROW_ERROR_EXCEPTION("Cell %v belongs to a different bundle %Qv",
                        metadataCell->GetCellBundle()->GetName());
                }
                cellBundle->SetMetadataCell(metadataCell->As<TChaosCell>());
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override
    {
        auto* cellBundle = GetThisImpl<TChaosCellBundle>();

        switch (key) {
            case EInternedAttributeKey::MetadataCellId:
                cellBundle->SetMetadataCell(nullptr);
                return true;

            default:
                break;
        }

        return TBase::RemoveBuiltinAttribute(key);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateChaosCellBundleProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TChaosCellBundle* cellBundle)
{
    return New<TChaosCellBundleProxy>(bootstrap, metadata, cellBundle);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosServer
