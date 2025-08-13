#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/object_client/proto/user_directory.pb.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

struct TSubjectDescriptor
{
    NSecurityClient::TSubjectId SubjectId;
    std::string Name;
    THashSet<std::string> Aliases;
    // Stores original subject names, not aliases.
    THashSet<std::string> RecursiveMemberOf;

    bool operator==(const TSubjectDescriptor& other) const = default;
};

using TSubjectDescriptorPtr = std::shared_ptr<const TSubjectDescriptor>;

using TGroupDescriptor = TSubjectDescriptor;
using TGroupDescriptorPtr = TSubjectDescriptorPtr;

struct TUserDescriptor
    : public TSubjectDescriptor
{
    TBooleanFormulaTags Tags;
    std::optional<int> ReadRequestRateLimit;
    std::optional<int> WriteRequestRateLimit;
    std::optional<int> QueueSizeLimit;

    bool operator==(const TUserDescriptor& other) const = default;
};

using TUserDescriptorPtr = std::shared_ptr<const TUserDescriptor>;

void FromProto(TSubjectDescriptor* subjectDescriptor, const NObjectClient::NProto::TSubjectDescriptor& proto);
void FromProto(TUserDescriptor* userDescriptor, const NObjectClient::NProto::TUserDescriptor& proto);

////////////////////////////////////////////////////////////////////////////////

std::optional<int> GetUserRequestRateLimit(const TUserDescriptor& descriptor, EUserWorkloadType workloadType);

////////////////////////////////////////////////////////////////////////////////

class TUserDirectory
    : public TRefCounted
{
public:
    TUserDescriptorPtr FindUserByName(const std::string& name) const;

    TUserDescriptorPtr FindUserByNameOrAlias(const std::string& name) const;
    TUserDescriptorPtr GetUserByNameOrAliasOrThrow(const std::string& name) const;

    TGroupDescriptorPtr FindGroupByNameOrAlias(const std::string& name) const;

    TSubjectDescriptorPtr GetSubjectByIdOrThrow(NSecurityClient::TSubjectId subjectId) const;

    std::vector<std::string> LoadFrom(
        std::vector<TUserDescriptor> users,
        std::vector<TGroupDescriptor> groups);

    void Clear();

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, SpinLock_);

    THashMap<std::string, TUserDescriptorPtr> NameOrAliasToUserDescriptor_;
    THashMap<std::string, TGroupDescriptorPtr> NameOrAliasToGroupDescriptor_;
    THashMap<NSecurityClient::TSubjectId, TSubjectDescriptorPtr> SubjectIdToDescriptor_;

    TSubjectDescriptorPtr FindSubjectByIdOrThrow(NSecurityClient::TSubjectId subjectId) const;
};

DEFINE_REFCOUNTED_TYPE(TUserDirectory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
