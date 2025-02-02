#pragma once

#include "object.h"
#include "public.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/security_server/acl.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for non-versioned objects with a tree-like structure.
template <class TSelf>
class TNonversionedMapObjectBase
    : public TObject
{
public:
    using TKeyToChild = THashMap<std::string, TRawObjectPtr<TSelf>>;
    using TChildToKey = THashMap<TRawObjectPtr<const TSelf>, std::string>;

    DEFINE_BYVAL_RW_PROPERTY(TRawObjectPtr<TSelf>, Parent);
    DEFINE_BYREF_RW_PROPERTY(TKeyToChild, KeyToChild);
    DEFINE_BYREF_RW_PROPERTY(TChildToKey, ChildToKey);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

    DEFINE_BYVAL_RW_PROPERTY(int, SubtreeSize, 1);

public:
    using TObject::TObject;
    explicit TNonversionedMapObjectBase(TObjectId id, bool isRoot = false);

    void ResetParent();
    //! Root objects cannot be attached to a parent but can have custom names.
    bool IsRoot() const;

    // TODO(kiselyovp) de-virtualise methods of TNonversionedMapObjectBase,
    // TNonversionedMapObjectProxyBase and TNonversionedMapObjectTypeHandlerBase

    //! Attaches a child to the object, the child must not have a parent.
    //! Does not change the ref count.
    virtual void AttachChild(const std::string& key, TSelf* child) noexcept;
    //! Unlinks a child from the object. Doesn't change the ref count.
    virtual void DetachChild(TSelf* child) noexcept;
    //! Changes the base name of a child of this object.
    virtual void RenameChild(TSelf* child, const std::string& newKey) noexcept;

    //! Finds a child by key. Returns |nullptr| if there is no such key.
    TSelf* FindChild(const std::string& key) const;
    //! Finds a child by key. It must be present.
    TSelf* GetChild(const std::string& key) const noexcept;
    //! Returns the key for a given child, |std::nullopt| if there is no such child.
    std::optional<std::string> FindChildKey(const TSelf* child) const noexcept;
    //! Returns the key for a given child, which must be present.
    std::string GetChildKey(const TSelf* child) const noexcept;

    //! Returns object's child key if it has a parent, the result of GetRootName()
    //! if it's a designated root or id preceded by a hash otherwise.
    std::string GetName() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

protected:
    bool IsRoot_ = false;

    //! Returns the name of a designated root object (id preceded by a hash by default).
    virtual std::string GetRootName() const;

private:
    TSelf* GetSelf();
    const TSelf* GetSelf() const;
};

////////////////////////////////////////////////////////////////////////////////

template <class TObject, class TFunctor>
void TraverseMapObjectSubtreeTopDown(TObject* object, TFunctor functor);

template <class TObject, class TResult, class TFunctor>
TResult AccumulateOverMapObjectSubtree(TObject* object, TResult init, TFunctor functor);

template <class TObject>
TObject* FindMapObjectLCA(TObject* lhs, TObject* rhs);

//! Calculates the subtree size for this object and all its descendants.
//! NB: Called one per root after loading a snapshot.
template <class TSelf>
void RecomputeSubtreeSize(TNonversionedMapObjectBase<TSelf>* mapObject, bool validateMatch);

extern template void RecomputeSubtreeSize(TNonversionedMapObjectBase<NSecurityServer::TAccount>* mapObject, bool validateMatch);
extern template void RecomputeSubtreeSize(TNonversionedMapObjectBase<NSchedulerPoolServer::TSchedulerPool>* mapObject, bool validateMatch);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

#define MAP_OBJECT_INL_H_
#include "map_object-inl.h"
#undef MAP_OBJECT_INL_H_
