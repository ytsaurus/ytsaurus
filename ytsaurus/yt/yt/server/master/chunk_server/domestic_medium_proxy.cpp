#include "domestic_medium_proxy.h"

#include "chunk_manager.h"
#include "config.h"
#include "domestic_medium.h"
#include "medium_proxy_base.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TDomesticMediumProxy
    : public TMediumProxyBase
{
public:
    using TMediumProxyBase::TMediumProxyBase;

private:
    using TBase = TMediumProxyBase;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        auto* medium = GetThisImpl<TDomesticMedium>();

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Transient)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Config)
            .SetWritable(true)
            .SetReplicated(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::DiskFamilyWhitelist)
            .SetWritable(true)
            .SetReplicated(true)
            .SetRemovable(true)
            .SetPresent(medium->DiskFamilyWhitelist().has_value()));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* medium = GetThisImpl<TDomesticMedium>();

        switch (key) {
            case EInternedAttributeKey::Transient:
                BuildYsonFluently(consumer)
                    .Value(medium->GetTransient());
                return true;

            case EInternedAttributeKey::Config:
                BuildYsonFluently(consumer)
                    .Value(medium->Config());
                return true;

            case EInternedAttributeKey::DiskFamilyWhitelist:
                if (!medium->DiskFamilyWhitelist()) {
                    break;
                }

                BuildYsonFluently(consumer)
                    .DoListFor(*medium->DiskFamilyWhitelist(), [] (TFluentList fluent, const TString& diskFamily) {
                        fluent.Item().Value(diskFamily);
                    });
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        auto* medium = GetThisImpl<TDomesticMedium>();
        const auto& chunkManager = Bootstrap_->GetChunkManager();

        switch (key) {
            case EInternedAttributeKey::Config: {
                auto config = ConvertTo<TDomesticMediumConfigPtr>(value);
                chunkManager->SetMediumConfig(medium, std::move(config));
                return true;
            }

            case EInternedAttributeKey::DiskFamilyWhitelist: {
                auto whitelist = ConvertTo<std::vector<TString>>(value);
                auto originalSize = whitelist.size();
                SortUnique(whitelist);
                if (whitelist.size() != originalSize) {
                    THROW_ERROR_EXCEPTION("Disk family whitelist must not contain duplicates");
                }
                medium->DiskFamilyWhitelist() = std::move(whitelist);
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override {
        auto* medium = GetThisImpl<TDomesticMedium>();

        switch (key) {
            case EInternedAttributeKey::DiskFamilyWhitelist:
                medium->DiskFamilyWhitelist().reset();
                return true;

            default:
                break;
        }

        return TBase::RemoveBuiltinAttribute(key);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateDomesticMediumProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TDomesticMedium* medium)
{
    return New<TDomesticMediumProxy>(bootstrap, metadata, medium);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
