#pragma once

#include "object.h"
#include "public.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/security_server/acl.h>

#include <yt/server/master/transaction_server/transaction.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for non-versioned objects with tree-like structure.
template <class TSelf>
class TNonversionedMapObjectBase
    : public TNonversionedObjectBase
{
public:
    using TKeyToChild = THashMap<TString, TSelf*>;
    using TChildToKey = THashMap<const TSelf*, TString>;

    DEFINE_BYREF_RW_PROPERTY(TKeyToChild, KeyToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToKey, ChildToKey);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

public:
    explicit TNonversionedMapObjectBase(TObjectId id);

    TSelf* GetParent() const;
    void SetParent(TSelf* parent);
    void ResetParent();

    // YYY(kiselyovp) okay, does literally EVERYTHING have to be virtual? I don't think this method does...
    //! Attaches a child to the object, the child must not have a parent.
    //! Does not change the ref count.
    virtual void AttachChild(const TString& key, TSelf* child) noexcept;
    //! Unlinks a child from the object. Doesn't change the ref count.
    virtual void DetachChild(TSelf* child) noexcept;
    //! Changes the base name of a child of this object.
    virtual void RenameChild(TSelf* child, const TString& newKey);

    //! Finds a child by key. Returns |nullptr| if there is no such key.
    TSelf* FindChild(const TString& key) const;
    //! Returns the key for a given child, |std::nullopt| if there is no such child.
    std::optional<TString> FindChildKey(const TSelf* child) const;
    //! Returns the key for a given child, which must be present.
    TString GetChildKey(const TSelf* child) const;

    //! Return object's child key or its id preceded by a hash if it has no parent.
    TString GetName() const;

    virtual void Save(NCellMaster::TSaveContext& context) const;
    virtual void Load(NCellMaster::TLoadContext& context);

private:
    TSelf* GetSelf();
    const TSelf* GetSelf() const;

    TSelf* Parent_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

#define MAP_OBJECT_INL_H_
#include "map_object-inl.h"
#undef MAP_OBJECT_INL_H_
