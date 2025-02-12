#pragma once

#include "public.h"
#include "acl.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

//! A named entity representing either a user or a group.
class TSubject
    : public NObjectServer::TObject
{
public:
    using TGroupSet = THashSet<TGroupRawPtr>;
    //! Set of groups containing this given subject.
    DEFINE_BYREF_RW_PROPERTY(TGroupSet, MemberOf);
    //! Transitive closure of the set of groups containing this given subject.
    DEFINE_BYREF_RW_PROPERTY(TGroupSet, RecursiveMemberOf);
    DEFINE_BYREF_RW_PROPERTY(THashSet<std::string>, Aliases);


    using TLinkedObjects = THashMap<NObjectServer::TObjectRawPtr, int>;
    //! Objects whose ACLs reference this particular subject, with counters.
    DEFINE_BYREF_RW_PROPERTY(TLinkedObjects, LinkedObjects);

    DEFINE_BYREF_RW_PROPERTY(TAccessControlDescriptor, Acd);

public:
    using TObject::TObject;
    explicit TSubject(TSubjectId id);

    const std::string& GetName() const;
    virtual void SetName(const std::string& name);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    bool IsUser() const;
    //! Casts the current instance to TUser.
    TUser* AsUser();

    bool IsGroup() const;
    //! Casts the current instance to TGroup.
    TGroup* AsGroup();

    //! Adds #object to |LinkedObjects| or increments the counter if it is already there.
    void LinkObject(TObject* object);
    //! Decrements the counter and removes #object from |LinkedObjects| if the counter drops to zero.
    void UnlinkObject(TObject* object);

protected:
    std::string Name_;
};

DEFINE_MASTER_OBJECT_TYPE(TSubject)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
