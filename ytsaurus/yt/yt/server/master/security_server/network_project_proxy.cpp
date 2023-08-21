#include "network_project_proxy.h"
#include "network_project.h"

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NYTree;
using namespace NYson;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TNetworkProjectProxy
    : public TNonversionedObjectProxyBase<TNetworkProject>
{
public:
    using TNonversionedObjectProxyBase::TNonversionedObjectProxyBase;

private:
    using TBase = TNonversionedObjectProxyBase<TNetworkProject>;

    void ValidateRemoval() override
    {
        const auto* networkProject = GetThisImpl();
        if (networkProject->IsBuiltin()) {
            THROW_ERROR_EXCEPTION("Cannot remove a built-in network project %Qv",
                networkProject->GetName());
        }
        ValidatePermission(EPermissionCheckScope::This, EPermission::Remove);
    }

    void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Name)
            .SetWritable(true));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ProjectId)
            .SetWritable(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        auto* networkProject = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::ProjectId:
                BuildYsonFluently(consumer)
                    .Value(networkProject->GetProjectId());
                return true;

            case EInternedAttributeKey::Name:
                BuildYsonFluently(consumer)
                    .Value(networkProject->GetName());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value) override
    {
        auto* networkProject = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::ProjectId:
                networkProject->SetProjectId(ConvertTo<ui32>(value));
                return true;

            case EInternedAttributeKey::Name:
                Bootstrap_->GetSecurityManager()->RenameNetworkProject(networkProject, ConvertTo<TString>(value));
                return true;

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateNetworkProjectProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TNetworkProject* networkProject)
{
    return New<TNetworkProjectProxy>(bootstrap, metadata, networkProject);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
