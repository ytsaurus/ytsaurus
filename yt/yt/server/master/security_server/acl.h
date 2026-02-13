#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/ytree/permission.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/yson/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct TAccessControlEntry
{
    TAccessControlEntry();
    TAccessControlEntry(
        ESecurityAction action,
        TSubject* subject,
        EPermissionSet permissions,
        EAceInheritanceMode inheritanceMode = EAceInheritanceMode::ObjectAndDescendants);

    // NB: Two ACEs being equal does not necessarily imply their complete
    // interchangeability. For one, the comparison has to ignore the order of
    // subjects for determinism. Moreover, subjects being linked or unlinked to
    // objects has no bearing on comparison.
    bool operator==(const TAccessControlEntry& rhs) const;

    static constexpr int TypicalSubjectCount = 4;

    ESecurityAction Action;
    TCompactVector<TSubjectRawPtr, TypicalSubjectCount> Subjects;
    EPermissionSet Permissions;
    EAceInheritanceMode InheritanceMode;
    std::optional<TBooleanFormula> SubjectTagFilter;
    std::optional<std::vector<std::string>> Columns;
    std::optional<bool> Vital;
    std::optional<std::string> RowAccessPredicate;
    std::optional<NSecurityClient::EInapplicableRowAccessPredicateMode> InapplicableRowAccessPredicateMode;

    void Persist(const NCellMaster::TPersistenceContext& context);
    void Persist(const NCypressServer::TCopyPersistenceContext& context);
};

void Serialize(const TAccessControlEntry& ace, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TAccessControlList
{
    using TEntries = std::vector<TAccessControlEntry>;

    TEntries Entries;

    void Persist(const NCellMaster::TPersistenceContext& context);
    void Persist(const NCypressServer::TCopyPersistenceContext& context);

    bool operator==(const TAccessControlList& other) const = default;
};

void Serialize(const TAccessControlList& acl, NYson::IYsonConsumer* consumer);

TAccessControlList DeserializeAclOrThrow(
    const NYTree::INodePtr& node,
    const ISecurityManagerPtr& securityManager);

struct TValidatedAccessControlList
{
    TAccessControlList Acl;
    std::vector<std::string> MissingSubjects;
    std::vector<std::string> PendingRemovalSubjects;
};

TValidatedAccessControlList DeserializeAclGatherMissingAndPendingRemovalSubjectsOrThrow(
    const NYTree::INodePtr& node,
    const ISecurityManagerPtr& securityManager,
    bool ignoreMissingSubjects,
    bool ignorePendingRemovalSubjects);

TAccessControlList DeserializeAclOrAlert(
    const NYTree::INodePtr& node,
    const ISecurityManagerPtr& securityManager);

////////////////////////////////////////////////////////////////////////////////

class TAccessControlDescriptor
{
    DEFINE_BYREF_RO_PROPERTY(TAccessControlList, Acl);
    DEFINE_BYREF_RO_PROPERTY(bool, Inherit, true);
    DEFINE_BYVAL_RO_PROPERTY(NObjectServer::TObjectRawPtr, Object);

public:
    explicit TAccessControlDescriptor(NObjectServer::TObject* object = nullptr);

    void SetInherit(bool inherit);

    void Clear();

    TSubject* GetOwner() const;
    void SetOwner(TSubject* owner);

    void AddEntry(const TAccessControlEntry& ace);
    void ClearEntries();
    void SetEntries(const TAccessControlList& acl);

    void OnSubjectDestroyed(TSubject* subject, TSubject* defaultOwner);

    void Persist(const NCellMaster::TPersistenceContext& context);
    void Persist(const NCypressServer::TCopyPersistenceContext& context);

private:
    TSubjectRawPtr Owner_;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

//! RAII wrapper for TAccessControlDescriptor that tracks modifications and notifies ISecurityManager.
class TMutableAccessControlDescriptorPtr
{
public:
    TMutableAccessControlDescriptorPtr(
        TAccessControlDescriptor* acd,
        ISecurityManager* securityManager);

    TMutableAccessControlDescriptorPtr(const TMutableAccessControlDescriptorPtr&) = delete;
    TMutableAccessControlDescriptorPtr& operator=(const TMutableAccessControlDescriptorPtr&) = delete;

    TMutableAccessControlDescriptorPtr(TMutableAccessControlDescriptorPtr&&) noexcept = default;
    TMutableAccessControlDescriptorPtr& operator=(TMutableAccessControlDescriptorPtr&&) noexcept = default;

    TAccessControlDescriptor* operator->();

    ~TMutableAccessControlDescriptorPtr();

    operator bool() const;

private:
    TAccessControlDescriptor* const Underlying_;
    ISecurityManager* const SecurityManager_;

    bool Modified_ = false;
};

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TWrappedAccessControlDescriptorPtr
{
public:
    TWrappedAccessControlDescriptorPtr(
        TAccessControlDescriptor* acd,
        ISecurityManager* securityManager);

    const TAccessControlDescriptor* operator->() const;

    [[nodiscard]] NDetail::TMutableAccessControlDescriptorPtr AsMutable();

    operator bool() const;

    TAccessControlDescriptor* Underlying();

private:
    TAccessControlDescriptor* Underlying_;
    ISecurityManager* SecurityManager_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
