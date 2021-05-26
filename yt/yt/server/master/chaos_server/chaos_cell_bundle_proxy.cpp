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
    TChaosCellBundleProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TChaosCellBundle* cellBundle)
        : TBase(bootstrap, metadata, cellBundle)
    { }

private:
    using TBase = TCellBundleProxy;

    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* attributes) override
    {
        attributes->push_back(TAttributeDescriptor(EInternedAttributeKey::ChaosOptions)
            .SetReplicated(true)
            .SetMandatory(true));

        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, IYsonConsumer* consumer) override
    {
        const auto* cellBundle = GetThisImpl<TChaosCellBundle>();

        switch (key) {
            case EInternedAttributeKey::ChaosOptions:
                BuildYsonFluently(consumer)
                    .Value(cellBundle->GetChaosOptions());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        return TBase::SetBuiltinAttribute(key, value);
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
