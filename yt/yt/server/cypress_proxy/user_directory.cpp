#include "user_directory.h"
#include "private.h"

#include <util/generic/scope.h>

namespace NYT::NCypressProxy {

using namespace NSecurityClient;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressProxyLogger;

////////////////////////////////////////////////////////////////////////////////

void FromProto(TSubjectDescriptor* subjectDescriptor, const NObjectClient::NProto::TSubjectDescriptor& proto)
{
    FromProto(&subjectDescriptor->SubjectId, proto.subject_id());
    subjectDescriptor->Name = proto.name();
    subjectDescriptor->Aliases = FromProto<THashSet<std::string>>(proto.aliases());
    subjectDescriptor->RecursiveMemberOf = FromProto<THashSet<std::string>>(proto.recursve_memeber_of());
}

void FromProto(TUserDescriptor* userDescriptor, const NObjectClient::NProto::TUserDescriptor& proto)
{
    FromProto(userDescriptor, proto.subject_descriptor());

    auto newTags = FromProto<THashSet<std::string>>(proto.tags());
    userDescriptor->Tags = TBooleanFormulaTags(std::move(newTags));

    userDescriptor->QueueSizeLimit = YT_OPTIONAL_FROM_PROTO(proto, request_queue_size_limit);
    userDescriptor->ReadRequestRateLimit = YT_OPTIONAL_FROM_PROTO(proto, read_request_rate_limit);
    userDescriptor->WriteRequestRateLimit = YT_OPTIONAL_FROM_PROTO(proto, write_request_rate_limit);

    userDescriptor->Banned = proto.banned();
}

////////////////////////////////////////////////////////////////////////////////

std::optional<int> GetUserRequestRateLimit(const TUserDescriptor& descriptor, EUserWorkloadType workloadType)
{
    switch (workloadType) {
        case EUserWorkloadType::Read:
            return descriptor.ReadRequestRateLimit;
        case EUserWorkloadType::Write:
            return descriptor.WriteRequestRateLimit;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

TUserDescriptorPtr TUserDirectory::FindUserByName(const std::string& name) const
{
    auto result = FindUserByNameOrAlias(name);
    return result && result->Name == name ? result : nullptr;
}

TUserDescriptorPtr TUserDirectory::FindUserByNameOrAlias(const std::string& nameOrAlias) const
{
    auto guard = ReaderGuard(SpinLock_);
    return GetOrDefault(NameOrAliasToUserDescriptor_, nameOrAlias);
}

TUserDescriptorPtr TUserDirectory::GetUserByNameOrAliasOrThrow(const std::string& nameOrAlias) const
{
    auto result = FindUserByNameOrAlias(nameOrAlias);
    if (!result) {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::AuthenticationError,
            "No such user %Qv",
            nameOrAlias);
    }
    return result;
}

TGroupDescriptorPtr TUserDirectory::FindGroupByNameOrAlias(const std::string& nameOrAlias) const
{
    auto guard = ReaderGuard(SpinLock_);
    return GetOrDefault(NameOrAliasToGroupDescriptor_, nameOrAlias);
}

TSubjectDescriptorPtr TUserDirectory::FindSubjectByIdOrThrow(NSecurityClient::TSubjectId subjectId) const
{
    auto guard = ReaderGuard(SpinLock_);
    return GetOrDefault(SubjectIdToDescriptor_, subjectId);
}

TSubjectDescriptorPtr TUserDirectory::GetSubjectByIdOrThrow(NSecurityClient::TSubjectId subjectId) const
{
    auto result = FindSubjectByIdOrThrow(subjectId);
    if (!result) {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::NoSuchSubject,
            "No such subject %v",
            subjectId);
    }
    return result;
}

const THashMap<std::string, TUserDescriptorPtr>& TUserDirectory::GetNameOrAliasToUserDescriptor() const
{
    return NameOrAliasToUserDescriptor_;
}

void TUserDirectory::Clear()
{
    auto guard = WriterGuard(SpinLock_);

    NameOrAliasToUserDescriptor_.clear();
    NameOrAliasToGroupDescriptor_.clear();
    SubjectIdToDescriptor_.clear();
}

std::vector<std::string> TUserDirectory::LoadFrom(
    std::vector<TUserDescriptor> users,
    std::vector<TGroupDescriptor> groups)
{
    THashMap<TSubjectId, TSubjectDescriptorPtr> subjectDescriptors;

    auto convertToMap = [&] <class TDescriptor> (std::vector<TDescriptor>&& descriptors) {
        THashMap<std::string, std::shared_ptr<const TDescriptor>> result;
        auto emplaceOrAlert = [&] (
            const std::string& alias,
            const std::shared_ptr<const TDescriptor>& descriptor)
        {
            auto [it, inserted] = result.emplace(alias, descriptor);
            if (!inserted) {
                YT_LOG_ALERT("Detected  name collision in user directory (SubjectIds: [%v, %v], Alias: %v)",
                    it->second->SubjectId,
                    descriptor->SubjectId,
                    alias);
            }
        };

        for (auto&& descriptor : descriptors) {
            auto descriptorHolder = std::make_shared<const TDescriptor>(std::move(descriptor));

            auto inserted = subjectDescriptors.emplace(descriptorHolder->SubjectId, descriptorHolder).second;
            if (!inserted) {
                YT_LOG_ALERT("Received multiple occurrences of subject descriptor (SubjectId: %v)",
                    descriptorHolder->SubjectId);
                continue;
            }

            for (const auto& alias : descriptorHolder->Aliases) {
                // Master guarantees that no duplicates occur.
                emplaceOrAlert(alias, descriptorHolder);
            }
            emplaceOrAlert(descriptorHolder->Name, descriptorHolder);
        }
        return result;
    };

    auto userDescriptors = convertToMap(std::move(users));
    auto groupDescriptors = convertToMap(std::move(groups));

    auto newUserDescriptors = GetValues(userDescriptors);

    {
        auto guard = WriterGuard(SpinLock_);
        std::swap(NameOrAliasToUserDescriptor_, userDescriptors);
        std::swap(NameOrAliasToGroupDescriptor_, groupDescriptors);
        std::swap(SubjectIdToDescriptor_, subjectDescriptors);
    }

    std::vector<std::string> updatedUsers;
    for (const auto& descriptor : newUserDescriptors) {
        auto it = userDescriptors.find(descriptor->Name);
        if (it == userDescriptors.end() || *it->second != *descriptor) {
            updatedUsers.push_back(descriptor->Name);
        }
    }

    return updatedUsers;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
