#pragma once

#include "private.h"
#include "group.h"
#include "user.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/public.h>

#include <yt/server/misc/interned_attributes.h>

#include <yt/server/object_server/object_detail.h>
#include <yt/server/object_server/public.h>

#include <yt/server/security_server/security_manager.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TSubjectProxy
    : public NObjectServer::TNonversionedObjectProxyBase<TImpl>
{
public:
    TSubjectProxy(
        NCellMaster::TBootstrap* bootstrap,
        NObjectServer::TObjectTypeMetadata* metadata,
        TImpl* subject)
        : TBase(bootstrap, metadata, subject)
    { }

private:
    typedef NObjectServer::TNonversionedObjectProxyBase<TImpl> TBase;

protected:
    virtual void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(NYTree::ISystemAttributeProvider::TAttributeDescriptor(EInternedAttributeKey::Name)
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back(EInternedAttributeKey::MemberOf);
        descriptors->push_back(EInternedAttributeKey::MemberOfClosure);
    }

    virtual bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* subject = this->GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Name:
                NYTree::BuildYsonFluently(consumer)
                    .Value(subject->GetName());
                return true;

            case EInternedAttributeKey::MemberOf:
                NYTree::BuildYsonFluently(consumer)
                    .DoListFor(subject->MemberOf(), [] (NYTree::TFluentList fluent, TGroup* group) {
                        fluent
                            .Item().Value(group->GetName());
                    });
                return true;

            case EInternedAttributeKey::MemberOfClosure:
                NYTree::BuildYsonFluently(consumer)
                    .DoListFor(subject->RecursiveMemberOf(), [] (NYTree::TFluentList fluent, TGroup* group) {
                        fluent
                            .Item().Value(group->GetName());
                    });
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    virtual bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value) override
    {
        auto* subject = this->GetThisImpl();
        const auto& securityManager = this->Bootstrap_->GetSecurityManager();

        switch (key) {
            case EInternedAttributeKey::Name: {
                auto newName = NYTree::ConvertTo<TString>(value);
                securityManager->RenameSubject(subject, newName);
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

