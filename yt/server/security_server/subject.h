#pragma once

#include "public.h"

#include <core/misc/property.h>

#include <server/object_server/object.h>

#include <server/cell_master/bootstrap.h>

namespace NYT {
namespace NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! A named entity representing either a user or a group.
class TSubject
    : public NObjectServer::TNonversionedObjectBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(Stroka, Name);

    typedef yhash_set<TGroup*> TGroupSet;
    //! Set of groups containing this given subject.
    DEFINE_BYREF_RW_PROPERTY(TGroupSet, MemberOf);
    //! Transitive closure of the set of groups containing this given subject.
    DEFINE_BYREF_RW_PROPERTY(TGroupSet, RecursiveMemberOf);

    typedef yhash_map<TObjectBase*, int> TLinkedObjects;
    //! Objects whose ACLs reference this particular subject, with counters.
    DEFINE_BYREF_RW_PROPERTY(TLinkedObjects, LinkedObjects);

public:
    explicit TSubject(const TSubjectId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    //! Casts the current instance to TUser.
    TUser* AsUser();

    //! Casts the current instance to TGroup.
    TGroup* AsGroup();

    //! Adds #object to |LinkedObjects| or increments the counter if it is already there.
    void LinkObject(TObjectBase* object);
    //! Decrements the counter and removes #object from |LinkedObjects| if the counter drops to zero.
    void UnlinkObject(TObjectBase* object);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
