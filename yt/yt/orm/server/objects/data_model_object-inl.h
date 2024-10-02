#ifndef DATA_MODEL_OBJECT_INL_H_
#error "Direct inclusion of this file is not allowed, include data_model_object.h"
// For the sake of sane code completion.
#include "data_model_object.h"
#endif

#include "db_schema.h"
#include "type_handler.h"

#include <yt/yt/orm/server/access_control/cast.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

namespace {

// #FromProto do not work with structures containing protos internally (e.g.: array of protos).
template <class TOriginal, class TSerialized>
void FromArrayOfProtos(
    std::vector<TOriginal>* originalArray,
    const std::vector<TSerialized>& serializedArray)
{
    originalArray->clear();
    originalArray->resize(serializedArray.size());
    for (size_t i = 0; i < serializedArray.size(); ++i) {
        FromProto(&(*originalArray)[i], serializedArray[i]);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class TProtoAccessControlEntry>
const TScalarAttributeDescriptor<
    TDataModelObject<TProtoAccessControlEntry>,
    typename TDataModelObject<TProtoAccessControlEntry>::TProtoAccessControlList>
    TDataModelObject<TProtoAccessControlEntry>::AclDescriptor{
    &ObjectsTable.Fields.MetaAcl,
    [] (TDataModelObject<TProtoAccessControlEntry>* object) { return &object->Acl(); }
};

////////////////////////////////////////////////////////////////////////////////

template <class TProtoAccessControlEntry>
TDataModelObject<TProtoAccessControlEntry>::TDataModelObject(
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : TObject(typeHandler, session)
    , Acl_(this, &AclDescriptor)
{ }

////////////////////////////////////////////////////////////////////////////////

template <class TProtoAccessControlEntry>
NAccessControl::TAccessControlList TDataModelObject<TProtoAccessControlEntry>::LoadOldAcl(
    std::source_location location) const
{
    NAccessControl::TAccessControlList result;
    FromArrayOfProtos(&result, Acl().LoadOld(location));
    return result;
}

template <class TProtoAccessControlEntry>
NAccessControl::TAccessControlList TDataModelObject<TProtoAccessControlEntry>::LoadAcl(
    std::source_location location) const
{
    NAccessControl::TAccessControlList result;
    FromArrayOfProtos(&result, Acl().Load(location));
    return result;
}

template <class TProtoAccessControlEntry>
void TDataModelObject<TProtoAccessControlEntry>::AddAce(
    const NAccessControl::TAccessControlEntry& ace,
    std::source_location location)
{
    TProtoAccessControlEntry protoAce;
    ToProto(&protoAce, ace);
    Acl().MutableLoad(/*sharedWrite*/ std::nullopt, location)->push_back(std::move(protoAce));
}

template <class TProtoAccessControlEntry>
void TDataModelObject<TProtoAccessControlEntry>::ScheduleAclLoad() const
{
    Acl_.ScheduleLoad();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
