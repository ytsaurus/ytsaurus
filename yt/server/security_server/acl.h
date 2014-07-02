#pragma once

#include "public.h"

#include <core/misc/small_vector.h>
#include <core/misc/property.h>

#include <core/ytree/permission.h>

#include <core/yson/public.h>

#include <server/cell_master/public.h>

#include <server/object_server/public.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

const int TypicalSubjectCount = 4;
typedef SmallVector<TSubject*, TypicalSubjectCount> TSubjectList;

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

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

void Serialize(const TAccessControlEntry& ace, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

const int TypicalAceCount = 16;
typedef SmallVector<TAccessControlEntry, TypicalAceCount> TAccessControlEntryList;

struct TAccessControlList
{
    TAccessControlEntryList Entries;
};

void Load(NCellMaster::TLoadContext& context, TAccessControlList& acl);
void Save(NCellMaster::TSaveContext& context, const TAccessControlList& acl);

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
    
    void Clear();

    TSubject* GetOwner() const;
    void SetOwner(TSubject* owner);

    void AddEntry(const TAccessControlEntry& ace);
    void ClearEntries();

    void OnSubjectDestroyed(TSubject* subject, TSubject* defaultOwner);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

private:
    TSubject* Owner_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
