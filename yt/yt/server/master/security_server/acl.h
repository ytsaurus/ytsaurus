#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/object_server/public.h>

#include <yt/server/master/cypress_server/public.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/small_vector.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/permission.h>
#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

const int TypicalSubjectCount = 4;
typedef SmallVector<TSubject*, TypicalSubjectCount> TSubjectList;

struct TAccessControlEntry
{
    TAccessControlEntry();
    TAccessControlEntry(
        ESecurityAction action,
        TSubject* subject,
        EPermissionSet permissions,
        EAceInheritanceMode inheritanceMode = EAceInheritanceMode::ObjectAndDescendants);

    ESecurityAction Action;
    TSubjectList Subjects;
    EPermissionSet Permissions;
    EAceInheritanceMode InheritanceMode;
    std::optional<std::vector<TString>> Columns;

    void Persist(NCellMaster::TPersistenceContext& context);
    void Persist(NCypressServer::TCopyPersistenceContext& context);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Save(NCypressServer::TBeginCopyContext& context) const;
    void Load(NCypressServer::TEndCopyContext& context);
};

void Serialize(const TAccessControlEntry& ace, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TAccessControlList
{
    std::vector<TAccessControlEntry> Entries;

    void Persist(NCellMaster::TPersistenceContext& context);
    void Persist(NCypressServer::TCopyPersistenceContext& context);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Save(NCypressServer::TBeginCopyContext& context) const;
    void Load(NCypressServer::TEndCopyContext& context);
};

void Serialize(const TAccessControlList& acl, NYson::IYsonConsumer* consumer);
void Deserialize(
    TAccessControlList& acl,
    const NYTree::INodePtr& node,
    const TSecurityManagerPtr& securityManager,
    // Puts missing subjects in this array. Throws an error on missing subjects if nullptr.
    std::vector<TString>* missingSubjects = nullptr);

////////////////////////////////////////////////////////////////////////////////

class TAccessControlDescriptor
{
    DEFINE_BYREF_RO_PROPERTY(TAccessControlList, Acl);
    DEFINE_BYVAL_RW_PROPERTY(bool, Inherit, true);
    DEFINE_BYVAL_RO_PROPERTY(NObjectServer::TObject*, Object);

public:
    explicit TAccessControlDescriptor(NObjectServer::TObject* object = nullptr);

    void Clear();

    TSubject* GetOwner() const;
    void SetOwner(TSubject* owner);

    void AddEntry(const TAccessControlEntry& ace);
    void ClearEntries();
    void SetEntries(const TAccessControlList& acl);

    void OnSubjectDestroyed(TSubject* subject, TSubject* defaultOwner);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    void Save(NCypressServer::TBeginCopyContext& context) const;
    void Load(NCypressServer::TEndCopyContext& context);

    void Persist(NCellMaster::TPersistenceContext& context);
    void Persist(NCypressServer::TCopyPersistenceContext& context);

private:
    TSubject* Owner_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
