#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/proto/user_directory.pb.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct TUserDescriptor
{
    std::string Name;
    std::optional<int> ReadRequestRateLimit;
    std::optional<int> WriteRequestRateLimit;
    std::optional<int> QueueSizeLimit;

    bool operator==(const TUserDescriptor& other) const = default;
};

void FromProto(TUserDescriptor* userDescriptor, const NObjectClient::NProto::TUserDescriptor& proto);
void ToProto(NObjectClient::NProto::TUserDescriptor* proto, const TUserDescriptor& userLimits);

////////////////////////////////////////////////////////////////////////////////

std::optional<int> GetUserRequestRateLimit(const TUserDescriptor& descriptor, EUserWorkloadType workloadType);

////////////////////////////////////////////////////////////////////////////////

class TUserDirectory
    : public TRefCounted
{
public:
    using TUserDescriptorPtr = std::shared_ptr<const TUserDescriptor>;
    TUserDescriptorPtr FindByName(const std::string& name) const;
    TUserDescriptorPtr GetUserByNameOrThrow(const std::string& name) const;

    std::vector<std::string> LoadFrom(const std::vector<TUserDescriptor>& sourceDirectory);

    void Clear();

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<std::string, TUserDescriptorPtr> NameToDescriptor_;
};

DEFINE_REFCOUNTED_TYPE(TUserDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
