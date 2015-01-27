#pragma once

#include "private.h"
#include "user.h"
#include "group.h"

#include <core/ytree/convert.h>

#include <core/ytree/fluent.h>

#include <server/object_server/object_detail.h>

#include <server/cell_master/public.h>
#include <server/cell_master/bootstrap.h>

#include <server/object_server/public.h>

#include <server/security_server/security_manager.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TSubjectProxy
    : public NObjectServer::TNonversionedObjectProxyBase<TImpl>
{
public:
    TSubjectProxy(NCellMaster::TBootstrap* bootstrap, TImpl* subject)
        : TBase(bootstrap, subject)
    { }

private:
    typedef NObjectServer::TNonversionedObjectProxyBase<TImpl> TBase;

protected:
    virtual NLog::TLogger CreateLogger() const override
    {
        return SecurityServerLogger;
    }

    virtual void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeInfo>* attributes) override
    {
        attributes->push_back("name");
        attributes->push_back("member_of");
        attributes->push_back("member_of_closure");
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override
    {
        const auto* subject = this->GetThisTypedImpl();

        if (key == "name") {
            NYTree::BuildYsonFluently(consumer)
                .Value(subject->GetName());
            return true;
        }

        if (key == "member_of") {
            NYTree::BuildYsonFluently(consumer)
                .DoListFor(subject->MemberOf(), [] (NYTree::TFluentList fluent, TGroup* group) {
                    fluent
                        .Item().Value(group->GetName());
                });
            return true;
        }

        if (key == "member_of_closure") {
            NYTree::BuildYsonFluently(consumer)
                .DoListFor(subject->RecursiveMemberOf(), [] (NYTree::TFluentList fluent, TGroup* group) {
                    fluent
                        .Item().Value(group->GetName());
                });
            return true;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(const Stroka& key, const NYTree::TYsonString& value) override
    {
        auto* subject = this->GetThisTypedImpl();
        auto securityManager = this->Bootstrap_->GetSecurityManager();

        if (key == "name") {
            auto newName = NYTree::ConvertTo<Stroka>(value);
            securityManager->RenameSubject(subject, newName);
            return true;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

