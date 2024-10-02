#pragma once

#include "public.h"

#include <yt/yt/core/misc/property.h>

namespace NYT::NOrm::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotSubject
{
public:
    TSnapshotSubject(TObjectId id, TObjectTypeValue type);
    virtual ~TSnapshotSubject() = default;

    TSnapshotUser* AsUser();
    TSnapshotGroup* AsGroup();

    DEFINE_BYVAL_RO_PROPERTY(TObjectId, Id);
    DEFINE_BYVAL_RO_PROPERTY(TObjectTypeValue, Type);
};

////////////////////////////////////////////////////////////////////////////////

class TSnapshotUser
    : public TSnapshotSubject
    , public TRefTracked<TSnapshotUser>
{
public:
    TSnapshotUser(
        TObjectId id,
        bool banned,
        std::optional<i64> requestWeightRateLimit,
        std::optional<i32> requestQueueSizeLimit,
        std::optional<std::string> executionPool);

    DEFINE_BYVAL_RO_PROPERTY(bool, Banned);
    DEFINE_BYVAL_RO_PROPERTY(std::optional<i64>, RequestWeightRateLimit);
    DEFINE_BYVAL_RO_PROPERTY(std::optional<i32>, RequestQueueSizeLimit);
    DEFINE_BYVAL_RO_PROPERTY(std::optional<std::string>, ExecutionPool);
};

////////////////////////////////////////////////////////////////////////////////

class TSnapshotGroup
    : public TSnapshotSubject
    , public TRefTracked<TSnapshotGroup>
{
public:
    TSnapshotGroup(TObjectId id, std::vector<TObjectId> members = {});

    bool ContainsUser(std::string_view userId) const;

    using TRecursiveUsers = THashSet<TObjectId, THash<TObjectId>, TEqualTo<>>;
    DEFINE_BYREF_RW_PROPERTY(TRecursiveUsers, RecursiveUserIds);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TObjectId>, Members);
};

////////////////////////////////////////////////////////////////////////////////

class TClusterSubjectSnapshot
    : public TRefCounted
{
public:
    TClusterSubjectSnapshot();

    void AddSubject(std::unique_ptr<TSnapshotSubject> subjectHolder);

    std::vector<TSnapshotSubject*> GetSubjects(TObjectTypeValue type) const;

    bool IsSuperuser(std::string_view userId) const;

    TSnapshotSubject* FindSubject(std::string_view id) const;

    TSnapshotGroup* GetSuperusersGroup() const;
    TSnapshotGroup* GetEveryoneGroup() const;

    void Prepare();

    std::optional<std::tuple<EAccessControlAction, TObjectId>> ApplyAcl(
        const TAccessControlList& acl,
        const NYPath::TYPath& attributePath,
        TAccessControlPermissionValue permission,
        std::string_view userId);

private:
    THashMap<TObjectId, std::unique_ptr<TSnapshotSubject>, THash<TObjectId>, TEqualTo<>> IdToSubject_;
    TSnapshotGroup* SuperusersGroup_ = nullptr;
    TSnapshotGroup* EveryoneGroup_ = nullptr;

    void ComputeRecursiveUsers(
        TSnapshotGroup* forGroup,
        TSnapshotGroup* currentGroup,
        THashSet<TSnapshotGroup*>* visitedGroups);

    std::optional<std::tuple<EAccessControlAction, TObjectId>> ApplyAce(
        const TAccessControlEntry& ace,
        TAccessControlPermissionValue permission,
        std::string_view userId);
};

DEFINE_REFCOUNTED_TYPE(TClusterSubjectSnapshot)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
