#pragma once

#include "object.h"

#include <yt/yt/orm/server/access_control/public.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

template <class TProtoAccessControlEntry>
class TDataModelObject
    : public TObject
{
public:
    TDataModelObject(
        IObjectTypeHandler* typeHandler,
        ISession* session);

    // Data model access control.
    using TProtoAccessControlList = std::vector<TProtoAccessControlEntry>;
    static const TScalarAttributeDescriptor<TDataModelObject, TProtoAccessControlList> AclDescriptor;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TProtoAccessControlList>, Acl);

    // ORM access control.
    NAccessControl::TAccessControlList LoadOldAcl(
        std::source_location location = std::source_location::current()) const override;
    NAccessControl::TAccessControlList LoadAcl(
        std::source_location location = std::source_location::current()) const override;
    void AddAce(
        const NAccessControl::TAccessControlEntry& ace,
        std::source_location location = std::source_location::current()) override;
    void ScheduleAclLoad() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects

#define DATA_MODEL_OBJECT_INL_H_
#include "data_model_object-inl.h"
#undef DATA_MODEL_OBJECT_INL_H_
