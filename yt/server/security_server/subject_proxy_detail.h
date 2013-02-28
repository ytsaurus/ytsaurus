#pragma once

#include "private.h"
#include "user.h"
#include "group.h"

#include <server/object_server/object_detail.h>

#include <server/cell_master/public.h>
#include <server/cell_master/bootstrap.h>

#include <server/object_server/public.h>

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
    virtual void ListSystemAttributes(std::vector<TAttributeInfo>* attributes) const override
    {
        attributes->push_back("name");
        attributes->push_back("member_of");
        attributes->push_back("member_of_closure");
        TBase::ListSystemAttributes(attributes);
    }

    virtual bool GetSystemAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) const override
    {
        const auto* subject = GetThisTypedImpl();

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

        return TBase::GetSystemAttribute(key, consumer);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

