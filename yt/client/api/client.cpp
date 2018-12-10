#include "client.h"

namespace NYT::NApi {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

EWorkloadCategory FromUserWorkloadCategory(EUserWorkloadCategory category)
{
    switch (category) {
        case EUserWorkloadCategory::Realtime:
            return EWorkloadCategory::UserRealtime;
        case EUserWorkloadCategory::Interactive:
            return EWorkloadCategory::UserInteractive;
        case EUserWorkloadCategory::Batch:
            return EWorkloadCategory::UserBatch;
        default:
            Y_UNREACHABLE();
    }
}

} // namespace

TUserWorkloadDescriptor::operator TWorkloadDescriptor() const
{
    TWorkloadDescriptor result;
    result.Category = FromUserWorkloadCategory(Category);
    result.Band = Band;
    return result;
}

struct TSerializableUserWorkloadDescriptor
    : public TYsonSerializableLite
{
    TUserWorkloadDescriptor Underlying;

    TSerializableUserWorkloadDescriptor()
    {
        RegisterParameter("category", Underlying.Category);
        RegisterParameter("band", Underlying.Band)
            .Optional();
    }
};

void Serialize(const TUserWorkloadDescriptor& workloadDescriptor, NYson::IYsonConsumer* consumer)
{
    TSerializableUserWorkloadDescriptor serializableWorkloadDescriptor;
    serializableWorkloadDescriptor.Underlying = workloadDescriptor;
    Serialize(serializableWorkloadDescriptor, consumer);
}

void Deserialize(TUserWorkloadDescriptor& workloadDescriptor, INodePtr node)
{
    TSerializableUserWorkloadDescriptor serializableWorkloadDescriptor;
    Deserialize(serializableWorkloadDescriptor, node);
    workloadDescriptor = serializableWorkloadDescriptor.Underlying;
}

////////////////////////////////////////////////////////////////////////////////

NRpc::TMutationId TMutatingOptions::GetOrGenerateMutationId() const
{
    if (Retry && !MutationId) {
        THROW_ERROR_EXCEPTION("Cannot execute retry without mutation id");
    }
    return MutationId ? MutationId : NRpc::GenerateMutationId();
}

////////////////////////////////////////////////////////////////////////////////

TError TCheckPermissionResult::ToError(const TString& user, EPermission permission) const
{
    switch (Action) {
        case NSecurityClient::ESecurityAction::Allow:
            return TError();

        case NSecurityClient::ESecurityAction::Deny: {
            TError error;
            if (ObjectName && SubjectName) {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied: %Qlv permission is denied for %Qv by ACE at %v",
                    permission,
                    *SubjectName,
                    *ObjectName);
            } else {
                error = TError(
                    NSecurityClient::EErrorCode::AuthorizationError,
                    "Access denied: %Qlv permission is not allowed by any matching ACE",
                    permission);
            }
            error.Attributes().Set("user", user);
            error.Attributes().Set("permission", permission);
            if (ObjectId) {
                error.Attributes().Set("denied_by", ObjectId);
            }
            if (SubjectId) {
                error.Attributes().Set("denied_for", SubjectId);
            }
            return error;
        }

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

