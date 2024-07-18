#include "user_directory.h"

#include <util/generic/scope.h>

namespace NYT::NCypressProxy {

using NObjectClient::NProto::TUserRequestLimits;

////////////////////////////////////////////////////////////////////////////////

void FromProto(TUserDescriptor* userDescriptor, const TUserRequestLimits& proto)
{
    userDescriptor->Name = proto.user_name();
    userDescriptor->QueueSizeLimit = YT_PROTO_OPTIONAL(proto, request_queue_size_limit);
    userDescriptor->ReadLimit = YT_PROTO_OPTIONAL(proto, read_request_rate_limit);
    userDescriptor->WriteLimit = YT_PROTO_OPTIONAL(proto, write_request_rate_limit);
}

void ToProto(TUserRequestLimits* proto, const TUserDescriptor& userLimits)
{
    if (userLimits.ReadLimit) {
        proto->set_read_request_rate_limit(*userLimits.ReadLimit);
    }
    if (userLimits.WriteLimit) {
        proto->set_write_request_rate_limit(*userLimits.WriteLimit);
    }
    if (userLimits.QueueSizeLimit) {
        proto->set_request_queue_size_limit(*userLimits.QueueSizeLimit);
    }
}

const TUserDescriptor& TUserDirectory::GetUserByNameOrThrow(const TString& name)
{
    auto guard = ReaderGuard(SpinLock_);

    auto it = NameToDescriptor_.find(name);
    if (it == NameToDescriptor_.end()) {
        THROW_ERROR_EXCEPTION("No such user %v", name);
    }
    return it->second;
}

void TUserDirectory::Clear()
{
    auto guard = WriterGuard(SpinLock_);

    NameToDescriptor_.clear();
}

void TUserDirectory::UpdateDescriptor(const TUserDescriptor& descriptor)
{
    auto guard = WriterGuard(SpinLock_);

    NameToDescriptor_[descriptor.Name] = descriptor;
}

std::vector<TString> TUserDirectory::LoadFrom(const std::vector<TUserDescriptor>& sourceDirectory)
{
    THashMap<TString, TUserDescriptor> userDescriptors;
    for (const auto& descriptor : sourceDirectory) {
        EmplaceOrCrash(userDescriptors, descriptor.Name, descriptor);
    }

    {
        auto guard = WriterGuard(SpinLock_);
        std::swap(NameToDescriptor_, userDescriptors);
    }

    std::vector<TString> updatedUsers;
    for (const auto& newDescriptor : sourceDirectory) {
        auto it = userDescriptors.find(newDescriptor.Name);
        if (it == userDescriptors.end() || it->second != newDescriptor) {
            updatedUsers.push_back(newDescriptor.Name);
        }
    }

    return updatedUsers;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
