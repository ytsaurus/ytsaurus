#include "user_directory.h"

#include <util/generic/scope.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

void FromProto(TUserDescriptor* userDescriptor, const NObjectClient::NProto::TUserDescriptor& proto)
{
    userDescriptor->Name = proto.user_name();
    userDescriptor->QueueSizeLimit = YT_OPTIONAL_FROM_PROTO(proto, request_queue_size_limit);
    userDescriptor->ReadRequestRateLimit = YT_OPTIONAL_FROM_PROTO(proto, read_request_rate_limit);
    userDescriptor->WriteRequestRateLimit = YT_OPTIONAL_FROM_PROTO(proto, write_request_rate_limit);
}

void ToProto(NObjectClient::NProto::TUserDescriptor* proto, const TUserDescriptor& userLimits)
{
    NYT::ToProto(proto->mutable_user_name(), userLimits.Name);
    if (userLimits.ReadRequestRateLimit) {
        proto->set_read_request_rate_limit(*userLimits.ReadRequestRateLimit);
    }
    if (userLimits.WriteRequestRateLimit) {
        proto->set_write_request_rate_limit(*userLimits.WriteRequestRateLimit);
    }
    if (userLimits.QueueSizeLimit) {
        proto->set_request_queue_size_limit(*userLimits.QueueSizeLimit);
    }
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

TUserDirectory::TUserDescriptorPtr TUserDirectory::FindByName(const std::string& name) const
{
    auto guard = ReaderGuard(SpinLock_);
    auto it = NameToDescriptor_.find(name);
    return it == NameToDescriptor_.end() ? nullptr : it->second;
}

TUserDirectory::TUserDescriptorPtr TUserDirectory::GetUserByNameOrThrow(const std::string& name) const
{
    auto result = FindByName(name);
    if (!result) {
        THROW_ERROR_EXCEPTION("No such user %Qv", name);
    }
    return result;
}

void TUserDirectory::Clear()
{
    auto guard = WriterGuard(SpinLock_);

    NameToDescriptor_.clear();
}

std::vector<std::string> TUserDirectory::LoadFrom(const std::vector<TUserDescriptor>& sourceDirectory)
{
    THashMap<std::string, TUserDescriptorPtr> userDescriptors;
    for (const auto& descriptor : sourceDirectory) {
        auto descriptorHolder = std::make_shared<TUserDescriptor>(descriptor);
        EmplaceOrCrash(userDescriptors, descriptor.Name, std::move(descriptorHolder));
    }

    {
        auto guard = WriterGuard(SpinLock_);
        std::swap(NameToDescriptor_, userDescriptors);
    }

    std::vector<std::string> updatedUsers;
    for (const auto& newDescriptor : sourceDirectory) {
        auto it = userDescriptors.find(newDescriptor.Name);
        if (it == userDescriptors.end() || *it->second != newDescriptor) {
            updatedUsers.push_back(newDescriptor.Name);
        }
    }

    return updatedUsers;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
