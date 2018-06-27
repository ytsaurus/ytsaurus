#include "object.h"
#include "db_schema.h"
#include "helpers.h"

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

const TScalarAttributeSchema<TObject, TObjectId> TObject::IdSchema{
    &ObjectsTable.Fields.Meta_Id
};

const TScalarAttributeSchema<TObject, TInstant> TObject::CreationTimeSchema{
    &ObjectsTable.Fields.Meta_CreationTime,
    [] (TObject* object) { return &object->CreationTime(); }
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
    , Labels_(this, &LabelsSchema)
    , Annotations_(this)
    , InheritAcl_(this, &InheritAclSchema)
    , Acl_(this, &AclSchema)
    , Id_(id)
    , TypeHandler_(typeHandler)
    , Session_(session)
    , ExistenceChecker_(this)
    , ParentIdAttribute_(this, parentId)
{ }

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

void TObject::ScheduleExists() const
{
    switch (State_) {
        case EObjectState::Normal:
        case EObjectState::Removing:
            ExistenceChecker_.ScheduleCheck();
            break;
        case EObjectState::Creating:
        case EObjectState::Created:
        case EObjectState::Removed:
        case EObjectState::CreatedRemoving:
        case EObjectState::CreatedRemoved:
        case EObjectState::Missing:
            break;
        default:
            Y_UNREACHABLE();
    }
}

bool TObject::Exists() const
{
    switch (State_) {
        case EObjectState::Normal:
        case EObjectState::Removing:
            return ExistenceChecker_.Check();
        case EObjectState::Creating:
        case EObjectState::Created:
        case EObjectState::CreatedRemoving:
            return true;
        case EObjectState::Removed:
        case EObjectState::CreatedRemoved:
        case EObjectState::Missing:
            return false;
        default:
            Y_UNREACHABLE();
    }
}

void TObject::ValidateExists() const
{
    if (!Exists()) {
        THROW_ERROR_EXCEPTION(
            NClient::NApi::EErrorCode::NoSuchObject,
            "%v %Qv does not exist",
            GetCapitalizedHumanReadableTypeName(GetType()),
            Id_);
    }
}

bool TObject::RemovalPending() const
{
    return State_ == EObjectState::Removing || State_ == EObjectState::CreatedRemoving;
}

void TObject::RegisterAttribute(IPersistentAttribute* attribute)
{
    Attributes_.push_back(attribute);
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
        "-_.";
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

} // namespace NObjects
} // namespace NServer
} // namespace NYP

