#include "group_proxy.h"
#include "group.h"
#include "security_manager.h"
#include "subject_proxy_detail.h"

#include <yt/server/misc/interned_attributes.h>

#include <yt/ytlib/security_client/group_ypath.pb.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TGroupProxy
    : public TSubjectProxy<TGroup>
{
public:
    TGroupProxy(
        NCellMaster::TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TGroup* group)
        : TBase(bootstrap, metadata, group)
    { }

private:
    typedef TSubjectProxy<TGroup> TBase;

    virtual void ValidateRemoval() override
    {
        const auto* group = GetThisImpl();
        if (group->IsBuiltin()) {
            THROW_ERROR_EXCEPTION("Cannot remove a built-in group %Qv",
                group->GetName());
        }
    }

    virtual void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(EInternedAttributeKey::Members);
    }

    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* group = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Members:
                BuildYsonFluently(consumer)
                    .DoListFor(group->Members(), [] (TFluentList fluent, TSubject* subject) {
                        fluent
                            .Item().Value(subject->GetName());
                    });
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(AddMember);
        DISPATCH_YPATH_SERVICE_METHOD(RemoveMember);
        return TBase::DoInvoke(context);
    }

    TSubject* GetSubject(const TString& name)
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* subject = securityManager->FindSubjectByName(name);
        if (!IsObjectAlive(subject)) {
            THROW_ERROR_EXCEPTION("No such user or group %Qv",
                name);
        }
        return subject;
    }

    DECLARE_YPATH_SERVICE_METHOD(NSecurityClient::NProto, AddMember)
    {
        Y_UNUSED(response);

        DeclareMutating();

        const auto& name = request->name();
        auto ignoreExisting = request->ignore_existing();

        context->SetRequestInfo("Name: %v, IgnoreExisting: %v",
            name,
            ignoreExisting);

        auto* member = GetSubject(name);
        auto* group = GetThisImpl();

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->AddMember(group, member, ignoreExisting);

        context->Reply();

        if (IsPrimaryMaster()) {
            PostToSecondaryMasters(context);
        }
    }

    DECLARE_YPATH_SERVICE_METHOD(NSecurityClient::NProto, RemoveMember)
    {
        Y_UNUSED(response);

        DeclareMutating();

        const auto& name = request->name();
        bool ignoreMissing = request->ignore_missing();

        context->SetRequestInfo("Name: %v, IgnoreMissing: %v",
            name,
            ignoreMissing);

        auto* member = GetSubject(name);
        auto* group = GetThisImpl();

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        securityManager->RemoveMember(group, member, ignoreMissing);

        context->Reply();

        if (IsPrimaryMaster()) {
            PostToSecondaryMasters(context);
        }
    }
};

IObjectProxyPtr CreateGroupProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TGroup* group)
{
    return New<TGroupProxy>(bootstrap, metadata, group);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

