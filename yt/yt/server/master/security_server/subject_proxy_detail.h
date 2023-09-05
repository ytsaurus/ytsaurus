#pragma once

#include "private.h"
#include "group.h"
#include "user.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/object_server/object_detail.h>
#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TSubjectProxy
    : public NObjectServer::TNonversionedObjectProxyBase<TImpl>
{
public:
    using NObjectServer::TNonversionedObjectProxyBase<TImpl>::TNonversionedObjectProxyBase;

private:
    using TBase = NObjectServer::TNonversionedObjectProxyBase<TImpl>;

protected:
    void ListSystemAttributes(std::vector<NYTree::ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(NYTree::ISystemAttributeProvider::TAttributeDescriptor(EInternedAttributeKey::Name)
            .SetWritable(true)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back(EInternedAttributeKey::MemberOf);
        descriptors->push_back(EInternedAttributeKey::MemberOfClosure);
        descriptors->push_back(NYTree::ISystemAttributeProvider::TAttributeDescriptor(EInternedAttributeKey::Aliases)
            .SetWritable(true)
            .SetReplicated(true));
    }

    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
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
            case EInternedAttributeKey::Aliases:
                NYTree::BuildYsonFluently(consumer)
                    .DoListFor(subject->Aliases(), [] (NYTree::TFluentList fluent, const TString& alias) {
                        fluent
                            .Item().Value(alias);
                    });
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override
    {
        auto* subject = this->GetThisImpl();
        const auto& securityManager = this->Bootstrap_->GetSecurityManager();

        switch (key) {
            case EInternedAttributeKey::Name: {
                auto newName = NYTree::ConvertTo<TString>(value);
                securityManager->RenameSubject(subject, newName);
                return true;
            }
            case EInternedAttributeKey::Aliases: {
                auto newAliases = NYTree::ConvertTo<std::vector<TString>>(value);
                securityManager->SetSubjectAliases(subject, newAliases);
                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

