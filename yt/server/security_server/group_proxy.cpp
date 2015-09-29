#include "stdafx.h"
#include "group_proxy.h"
#include "group.h"
#include "security_manager.h"
#include "subject_proxy_detail.h"

#include <core/ytree/fluent.h>

#include <ytlib/security_client/group_ypath.pb.h>

namespace NYT {
namespace NSecurityServer {

using namespace NYTree;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TGroupProxy
    : public TSubjectProxy<TGroup>
{
public:
    TGroupProxy(NCellMaster::TBootstrap* bootstrap, TGroup* group)
        : TBase(bootstrap, group)
    { }

private:
    typedef TSubjectProxy<TGroup> TBase;

    virtual void ValidateRemoval() override
    {
        const auto* group = GetThisTypedImpl();
        if (group->IsBuiltin()) {
            THROW_ERROR_EXCEPTION("Cannot remove a built-in group %Qv",
                group->GetName());
        }
    }

    virtual void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back("members");
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override
    {
        const auto* group = GetThisTypedImpl();

        if (key == "members") {
            BuildYsonFluently(consumer)
                .DoListFor(group->Members(), [] (TFluentList fluent, TSubject* subject) {
                    fluent
                        .Item().Value(subject->GetName());
                });
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(AddMember);
        DISPATCH_YPATH_SERVICE_METHOD(RemoveMember);
        return TBase::DoInvoke(context);
    }

    TSubject* GetSubject(const Stroka& name)
    {
        auto securityManager = Bootstrap_->GetSecurityManager();
        auto* subject = securityManager->FindSubjectByName(name);
        if (!IsObjectAlive(subject)) {
            THROW_ERROR_EXCEPTION("No such user or group %Qv",
                name);
        }
        return subject;
    }

    DECLARE_YPATH_SERVICE_METHOD(NSecurityClient::NProto, AddMember)
    {
        UNUSED(response);

        DeclareMutating();

        const auto& name = request->name();

        context->SetRequestInfo("Name: %v", name);

        auto* member = GetSubject(name);
        auto* group = GetThisTypedImpl();

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->AddMember(group, member);

        context->Reply();

        if (IsPrimaryMaster()) {
            PostToSecondaryMasters(context);
        }
    }

    DECLARE_YPATH_SERVICE_METHOD(NSecurityClient::NProto, RemoveMember)
    {
        UNUSED(response);

        DeclareMutating();

        const auto& name = request->name();

        context->SetRequestInfo("Name: %v", name);

        auto* member = GetSubject(name);
        auto* group = GetThisTypedImpl();

        auto securityManager = Bootstrap_->GetSecurityManager();
        securityManager->RemoveMember(group, member);

        context->Reply();

        if (IsPrimaryMaster()) {
            PostToSecondaryMasters(context);
        }
    }
};

IObjectProxyPtr CreateGroupProxy(
    NCellMaster::TBootstrap* bootstrap,
    TGroup* group)
{
    return New<TGroupProxy>(bootstrap, group);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

