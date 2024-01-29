#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/ytree/permission.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/yson/public.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

const int TypicalSubjectCount = 4;
using TSubjectList = TCompactVector<TSubject*, TypicalSubjectCount>;

struct TAccessControlEntry
{
    TAccessControlEntry();
    TAccessControlEntry(
        ESecurityAction action,
        TSubject* subject,
        EPermissionSet permissions,
        EAceInheritanceMode inheritanceMode = EAceInheritanceMode::ObjectAndDescendants);

    // NB: two ACEs being equal does not necessarily imply their complete
    // interchangeability. For one, the comparison has to ignore the order of
    // subjects for determinism. Moreover, subjects being linked or unlinked to
    // objects has no bearing on comparison.
    bool operator==(const TAccessControlEntry& rhs) const;

    ESecurityAction Action;
    TSubjectList Subjects;
    EPermissionSet Permissions;
    EAceInheritanceMode InheritanceMode;
    std::optional<TBooleanFormula> SubjectTagFilter;
    std::optional<std::vector<TString>> Columns;
    std::optional<bool> Vital;

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

std::pair<TAccessControlList, std::vector<TString>>
DeserializeAclGatherMissingSubjectsOrThrow(
    const NYTree::INodePtr& node,
    const ISecurityManagerPtr& securityManager);

TAccessControlList DeserializeAclOrAlert(
    const NYTree::INodePtr& node,
    const ISecurityManagerPtr& securityManager);

////////////////////////////////////////////////////////////////////////////////

class TAccessControlDescriptor
{
    DEFINE_BYREF_RO_PROPERTY(TAccessControlList, Acl);
    DEFINE_BYREF_RO_PROPERTY(bool, Inherit, true);
    DEFINE_BYVAL_RO_PROPERTY(NObjectServer::TObject*, Object);

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
    TSubject* Owner_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
