#include "object.h"

#include "attribute_schema.h"
#include "db_schema.h"
#include "helpers.h"

#include "type_handler.h"

#include <yp/server/lib/objects/type_info.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TObject, TInstant> TObject::CreationTimeSchema{
    &ObjectsTable.Fields.Meta_CreationTime,
    [] (TObject* object) { return &object->CreationTime(); }
};

const TScalarAttributeSchema<TObject, TObject::TMetaEtc> TObject::MetaEtcSchema{
    &ObjectsTable.Fields.Meta_Etc,
    [] (TObject* object) { return &object->MetaEtc(); }
};

const TScalarAttributeSchema<TObject, NYT::NYTree::IMapNodePtr> TObject::LabelsSchema{
    &ObjectsTable.Fields.Labels,
    [] (TObject* object) { return &object->Labels(); }
};

const TScalarAttributeSchema<TObject, bool> TObject::InheritAclSchema{
    &ObjectsTable.Fields.Meta_InheritAcl,
    [] (TObject* object) { return &object->InheritAcl(); }
};

const TScalarAttributeSchema<TObject, TObject::TAcl> TObject::AclSchema{
    &ObjectsTable.Fields.Meta_Acl,
    [] (TObject* object) { return &object->Acl(); }
};

TObject::TObject(
    const TObjectId& id,
    const TObjectId& parentId,
    IObjectTypeHandler* typeHandler,
    ISession* session)
    : CreationTime_(this, &CreationTimeSchema)
    , MetaEtc_(this, &MetaEtcSchema)
    , Labels_(this, &LabelsSchema)
    , Annotations_(this)
    , InheritAcl_(this, &InheritAclSchema)
    , Acl_(this, &AclSchema)
    , Id_(id)
    , TypeHandler_(typeHandler)
    , Session_(session)
    , ExistenceChecker_(this)
    , TombstoneChecker_(this)
    , ParentIdAttribute_(this, parentId)
{ }

void TObject::InitializeCreating()
{
    State_ = EObjectState::Creating;
}

void TObject::InitializeInstantiated()
{
    State_ = EObjectState::Instantiated;
    ExistenceChecker_.ScheduleCheck();
    InheritAcl_.ScheduleLoad();
    Acl_.ScheduleLoad();
}

const TObjectId& TObject::GetId() const
{
    return Id_;
}

const TObjectId& TObject::GetParentId() const
{
    return ParentIdAttribute_.GetId();
}

IObjectTypeHandler* TObject::GetTypeHandler() const
{
    return TypeHandler_;
}

ISession* TObject::GetSession() const
{
    return Session_;
}

void TObject::Remove()
{
    Session_->RemoveObject(this);
}

bool TObject::DoesExist() const
{
    switch (State_) {
        case EObjectState::Instantiated:
        case EObjectState::Removing:
            return ExistenceChecker_.Check();
        case EObjectState::Creating:
        case EObjectState::Created:
        case EObjectState::CreatedRemoving:
            return true;
        case EObjectState::Removed:
        case EObjectState::CreatedRemoved:
            return false;
        default:
            YT_ABORT();
    }
}

bool TObject::DidExist() const
{
    switch (State_) {
        case EObjectState::Instantiated:
        case EObjectState::Removing:
        case EObjectState::Removed:
            return ExistenceChecker_.Check();
        case EObjectState::Creating:
        case EObjectState::Created:
        case EObjectState::CreatedRemoving:
        case EObjectState::CreatedRemoved:
            return false;
        default:
            YT_ABORT();
    }
}

void TObject::ValidateExists() const
{
    if (!DoesExist()) {
        THROW_ERROR_EXCEPTION(
            NClient::NApi::EErrorCode::NoSuchObject,
            "%v %v does not exist",
            GetCapitalizedHumanReadableTypeName(GetType()),
            GetObjectDisplayName(this));
    }
}

void TObject::ScheduleTombstoneCheck()
{
    TombstoneChecker_.ScheduleCheck();
}

bool TObject::IsTombstone() const
{
    return TombstoneChecker_.Check();
}

bool TObject::IsRemoving() const
{
    return State_ == EObjectState::Removing || State_ == EObjectState::CreatedRemoving;
}

bool TObject::IsBuiltin() const
{
    return false;
}

void TObject::RegisterAttribute(IPersistentAttribute* attribute)
{
    Attributes_.push_back(attribute);
}

void TObject::ScheduleStore()
{
    StoreScheduled_ = true;
}

bool TObject::IsStoreScheduled() const
{
    return StoreScheduled_;
}

bool TObject::HasHistoryEnabledAttributes() const
{
    return TypeHandler_->HasHistoryEnabledAttributes();
}

NYT::NYson::TYsonString TObject::GetHistoryEnabledAttributes()
{
    return NYT::NYTree::ConvertToYsonString(TypeHandler_->GetRootAttributeSchema()->GetHistoryEnabledAttributes(this));
}

////////////////////////////////////////////////////////////////////////////////

TClusterTag ClusterTagFromId(const TTransactionId& id)
{
    return static_cast<TClusterTag>((id.Parts32[1] >> 16) & 0x00ff);
}

TMasterInstanceTag MasterInstanceTagFromId(const TTransactionId& id)
{
    return static_cast<TMasterInstanceTag>((id.Parts32[1] >> 24) & 0x00ff);
}

TObjectId GetObjectId(TObject* object)
{
    return object ? object->GetId() : TObjectId();
}

void ValidateObjectId(EObjectType type, const TObjectId& id)
{
    if (id.empty()) {
        THROW_ERROR_EXCEPTION(
            NClient::NApi::EErrorCode::InvalidObjectId,
            "Object id cannot be empty");
    }

    if (id.length() > NClient::NApi::MaxObjectIdLength) {
        THROW_ERROR_EXCEPTION(
            NClient::NApi::EErrorCode::InvalidObjectId,
            "Object id length cannot exceed %v",
            NClient::NApi::MaxObjectIdLength);
    }

    static const char ValidGenericChars[] =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "-_.:";
    static const char ValidDnsChars[] =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "-";

    const auto* validChars =
        type == EObjectType::Pod
        ? ValidDnsChars
        : ValidGenericChars;
    for (char ch : id) {
        if (!strchr(validChars, ch)) {
            THROW_ERROR_EXCEPTION(
                NClient::NApi::EErrorCode::InvalidObjectId,
                "Object id %Qv of type %Qlv contains invalid symbol %Qv",
                id,
                type,
                ch);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

