#include "chunk_location_proxy.h"

#include "chunk_location.h"
#include "chunk_manager.h"
#include "domestic_medium.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/node_tracker_server/node.h>
#include <yt/yt/server/master/node_tracker_server/node_statistics_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/yson_string/convert.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;
using namespace NNodeTrackerServer;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TChunkLocationProxy
    : public TNonversionedObjectProxyBase<TRealChunkLocation>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TRealChunkLocation>;

    void ValidateRemoval() override
    {
        const auto* location = GetThisImpl();
        if (!location->GetNode()) {
            THROW_ERROR_EXCEPTION("Location is not bound to any node");
        }
        if (location->GetState() == EChunkLocationState::Online) {
            THROW_ERROR_EXCEPTION("Location is online");
        }
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        const auto* location = GetThisImpl();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::NodeAddress)
            .SetPresent(IsObjectAlive(location->GetNode())));
        descriptors->push_back(EInternedAttributeKey::State);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::MediumOverride)
            .SetPresent(static_cast<bool>(location->MediumOverride()))
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true));
        descriptors->push_back(EInternedAttributeKey::Statistics);
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Uuid)
            .SetReplicated(true)
            .SetMandatory(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* location = GetThisImpl();

        const auto& chunkManager = Bootstrap_->GetChunkManager();

        switch (key) {
            case EInternedAttributeKey::NodeAddress: {
                const auto* node = location->GetNode();
                if (!IsObjectAlive(node)) {
                    return false;
                }
                BuildYsonFluently(consumer)
                    .Value(node->GetDefaultAddress());
                return true;
            }

            case EInternedAttributeKey::State:
                BuildYsonFluently(consumer)
                    .Value(location->GetState());
                return true;

            case EInternedAttributeKey::MediumOverride:
                if (const auto& mediumOverride = location->MediumOverride()) {
                    BuildYsonFluently(consumer)
                        .Value(mediumOverride->GetName());
                    return true;
                } else {
                    return false;
                }

            case EInternedAttributeKey::Statistics:
                BuildYsonFluently(consumer)
                    .BeginMap()
                        .Do([&] (auto fluent) {
                            NNodeTrackerServer::Serialize(location->Statistics(), fluent, chunkManager);
                        })
                    .EndMap();
                return true;

            case EInternedAttributeKey::Uuid:
                BuildYsonFluently(consumer)
                    .Value(location->GetUuid());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        auto* location = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::MediumOverride: {
                auto mediumName = ConvertTo<TString>(value);
                const auto& chunkManager = Bootstrap_->GetChunkManager();
                auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
                if (medium->IsOffshore()) {
                    THROW_ERROR_EXCEPTION("Chunk location medium cannot be overridden with offshore medium")
                        << TErrorAttribute("location_id", location->GetId())
                        << TErrorAttribute("medium_index", medium->GetIndex())
                        << TErrorAttribute("medium_name", medium->GetName())
                        << TErrorAttribute("medium_type", medium->GetType());
                }

                auto* domesticMedium = medium->AsDomestic();
                const auto& whitelist = domesticMedium->DiskFamilyWhitelist();
                if (whitelist.has_value() &&
                    !std::binary_search(
                        whitelist->begin(),
                        whitelist->end(),
                        location->Statistics().disk_family()))
                {
                    THROW_ERROR_EXCEPTION("Inconsistent medium override: location's disk family %Qv is absent from medium %Qv whitelist",
                        location->Statistics().disk_family(),
                        mediumName)
                        << TErrorAttribute("location_id", location->GetId())
                        << TErrorAttribute("medium_family_whitelist", whitelist);
                }
                location->MediumOverride() = TDomesticMediumPtr(domesticMedium);

                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override
    {
        auto* location = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::MediumOverride:
                location->MediumOverride().Reset();
                return true;

            default:
                break;
        }

        return TBase::RemoveBuiltinAttribute(key);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateChunkLocationProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TRealChunkLocation* location)
{
    return New<TChunkLocationProxy>(bootstrap, metadata, location);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
