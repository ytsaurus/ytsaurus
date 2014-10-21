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

    virtual NLog::TLogger CreateLogger() const override
    {
        return SecurityServerLogger;
    }

    virtual void ValidateRemoval() override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        const auto* group = GetThisTypedImpl();
        if (group == securityManager->GetEveryoneGroup() ||
            group == securityManager->GetUsersGroup())
        {
            THROW_ERROR_EXCEPTION("Cannot remove a built-in group");
        }
    }

    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) override
    {
        attributes->push_back("members");
        TBase::ListSystemAttributes(attributes);
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
        auto securityManager = Bootstrap->GetSecurityManager();
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

        context->SetResponseInfo("Name: %v",
            ~request->name());

        auto securityManager = Bootstrap->GetSecurityManager();

        auto* member = GetSubject(request->name());
        auto* group = GetThisTypedImpl();
        securityManager->AddMember(group, member);

        context->Reply();
    }

    DECLARE_YPATH_SERVICE_METHOD(NSecurityClient::NProto, RemoveMember)
    {
        UNUSED(response);

        DeclareMutating();

        context->SetResponseInfo("Name: %v",
            ~request->name());

        auto securityManager = Bootstrap->GetSecurityManager();

        auto* member = GetSubject(request->name());
        auto* group = GetThisTypedImpl();
        securityManager->RemoveMember(group, member);

        context->Reply();
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

