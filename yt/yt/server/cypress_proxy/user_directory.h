#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/proto/user_directory.pb.h>

#include <yt/yt/core/rpc/per_user_request_queue_provider.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct TUserDescriptor
{
    TString Name;
    std::optional<int> ReadLimit;
    std::optional<int> WriteLimit;
    std::optional<int> QueueSizeLimit;

    bool operator==(const TUserDescriptor& other) const = default;
};

void FromProto(TUserDescriptor* userDescriptor, const NObjectClient::NProto::TUserRequestLimits& proto);
void ToProto(NObjectClient::NProto::TUserRequestLimits* proto, const TUserDescriptor& userLimits);

////////////////////////////////////////////////////////////////////////////////

class TUserDirectory
    : public TRefCounted
{
public:
    void UpdateDescriptor(const TUserDescriptor& descriptor);

    const TUserDescriptor& GetUserByNameOrThrow(const TString& name);

    std::vector<TString> LoadFrom(const std::vector<TUserDescriptor>& sourceDirectory);

    void Clear();

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);
    THashMap<TString, TUserDescriptor> NameToDescriptor_;
};

DEFINE_REFCOUNTED_TYPE(TUserDirectory)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TUserDirectory& mediumDirectory, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
