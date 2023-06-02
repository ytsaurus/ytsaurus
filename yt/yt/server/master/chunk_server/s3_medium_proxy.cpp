#include "s3_medium_proxy.h"

#include "config.h"
#include "medium_proxy_base.h"
#include "s3_medium.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NChunkServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TS3MediumProxy
    : public TMediumProxyBase
{
public:
    using TMediumProxyBase::TMediumProxyBase;

private:
    using TBase = TMediumProxyBase;

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Config)
            .SetWritable(true)
            .SetReplicated(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* medium = GetThisImpl<TS3Medium>();
        YT_VERIFY(medium->Config());

        switch (key) {
            case EInternedAttributeKey::Config:
                BuildYsonFluently(consumer)
                    .Value(medium->Config());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        auto* medium = GetThisImpl<TS3Medium>();

        switch (key) {
            case EInternedAttributeKey::Config: {
                auto config = ConvertTo<TS3MediumConfigPtr>(value);
                medium->Config() = std::move(config);
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateS3MediumProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TS3Medium* medium)
{
    return New<TS3MediumProxy>(bootstrap, metadata, medium);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
