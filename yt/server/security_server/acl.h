#pragma once

#include "public.h"

#include <ytlib/misc/small_vector.h>
#include <ytlib/misc/property.h>

#include <ytlib/ytree/permission.h>

#include <ytlib/yson/public.h>

#include <server/cell_master/public.h>

#include <server/object_server/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

const int TypicalSubjectCount = 4;
typedef TSmallVector<TSubject*, TypicalSubjectCount> TSubjectList;

struct TAccessControlEntry
{
    TAccessControlEntry();
    TAccessControlEntry(
        ESecurityAction action,
        TSubject* subject,
        EPermissionSet permissions);

    ESecurityAction Action;
    TSubjectList Subjects;
    EPermissionSet Permissions;
};

void Load(const NCellMaster::TLoadContext& context, TAccessControlEntry& entry);
void Save(const TAccessControlEntry& entry, const NCellMaster::TSaveContext& context);

void Serialize(const TAccessControlEntry& ace, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

const int TypicalAceCount = 16;
typedef TSmallVector<TAccessControlEntry, TypicalAceCount> TAccessControlEntryList;

struct TAccessControlList
{
    TAccessControlEntryList Entries;
};

void Load(const NCellMaster::TLoadContext& context, TAccessControlList& acl);
void Save(const NCellMaster::TSaveContext& context, const TAccessControlList& acl);

void Serialize(const TAccessControlList& acl, NYson::IYsonConsumer* consumer);
void Deserilize(
    TAccessControlList& acl,
    EPermissionSet supportedPermissions,
    NYTree::INodePtr node,
    TSecurityManagerPtr securityManager);

////////////////////////////////////////////////////////////////////////////////

class TAccessControlDescriptor
{
    DEFINE_BYREF_RO_PROPERTY(TAccessControlList, Acl);
    DEFINE_BYVAL_RW_PROPERTY(bool, Inherit);
    DEFINE_BYVAL_RO_PROPERTY(NObjectServer::TObjectBase*, Object);

public:
    explicit TAccessControlDescriptor(NObjectServer::TObjectBase* object);
    ~TAccessControlDescriptor();

    TSubject* GetOwner() const;
    void SetOwner(TSubject* owner);

    void AddEntry(const TAccessControlEntry& ace);
    void ClearEntries();

    void OnSubjectDestroyed(TSubject* subject, TSubject* defaultOwner);

private:
    TSubject* Owner_;

    friend void Load(const NCellMaster::TLoadContext& context, TAccessControlDescriptor& acd);
    friend void Save(const NCellMaster::TSaveContext& context, const TAccessControlDescriptor& acd);

};

void Load(const NCellMaster::TLoadContext& context, TAccessControlDescriptor& acd);
void Save(const NCellMaster::TSaveContext& context, const TAccessControlDescriptor& acd);

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
