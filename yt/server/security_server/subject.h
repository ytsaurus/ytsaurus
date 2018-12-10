#pragma once

#include "public.h"
#include "acl.h"

#include <yt/server/cell_master/bootstrap.h>

#include <yt/server/object_server/object.h>

#include <yt/core/misc/property.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! A named entity representing either a user or a group.
class TSubject
    : public NObjectServer::TNonversionedObjectBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);

    typedef THashSet<TGroup*> TGroupSet;
    //! Set of groups containing this given subject.
    DEFINE_BYREF_RW_PROPERTY(TGroupSet, MemberOf);
    //! Transitive closure of the set of groups containing this given subject.
    DEFINE_BYREF_RW_PROPERTY(TGroupSet, RecursiveMemberOf);

    typedef THashMap<TObjectBase*, int> TLinkedObjects;
    //! Objects whose ACLs reference this particular subject, with counters.
    DEFINE_BYREF_RW_PROPERTY(TLinkedObjects, LinkedObjects);

    DEFINE_BYREF_RW_PROPERTY(TAccessControlDescriptor, Acd);

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

} // namespace NYT::NSecurityServer
