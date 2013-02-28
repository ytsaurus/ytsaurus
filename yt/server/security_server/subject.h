#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

#include <server/object_server/object.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! A named entity representing either a user or a group.
class TSubject
    : public NObjectServer::TNonversionedObjectBase
{
    DEFINE_BYVAL_RW_PROPERTY(Stroka, Name);

    typedef yhash_set<TGroup*> TGroupSet;
    //! Set of groups containing this given subject.
    DEFINE_BYREF_RW_PROPERTY(TGroupSet, MemberOf);
    //! Transitive closure of the set of groups containing this given subject.
    DEFINE_BYREF_RW_PROPERTY(TGroupSet, RecursiveMemberOf);

    typedef yhash_set<TObjectBase*> TObjectSet;
    //! Objects whose ACLs reference this particular subject.
    DEFINE_BYREF_RW_PROPERTY(TObjectSet, ReferencingObjects);

public:
    explicit TSubject(const TSubjectId& id);

    void Save(const NCellMaster::TSaveContext& context) const;
    void Load(const NCellMaster::TLoadContext& context);

    //! Casts the current instance to TUser.
    TUser* AsUser();

    //! Casts the current instance to TGroup.
    TGroup* AsGroup();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
