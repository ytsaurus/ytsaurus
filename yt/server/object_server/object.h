#pragma once

#include "public.h"
#include "attribute_set.h"

#include <server/hydra/entity_map.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for all objects in YT master server.
class TObjectBase
    : public NHydra::TEntityBase
{
public:
    explicit TObjectBase(const TObjectId& id);
    virtual ~TObjectBase();

    //! Marks the object as destroyed.
    void SetDestroyed();

    //! Returns the object id.
    const TObjectId& GetId() const;

    //! Returns the object type.
    EObjectType GetType() const;

    //! Returns |true| if this is a well-known subject (e.g. "root", "users" etc).
    bool IsBuiltin() const;


    //! Increments the object's reference counter by one.
    /*!
     *  \returns the incremented counter.
     */
    int RefObject();

    //! Decrements the object's reference counter by #count.
    /*!
     *  \note
     *  Objects do not self-destruct, it's callers responsibility to check
     *  if the counter reaches zero.
     *
     *  \returns the decremented counter.
     */
    int UnrefObject(int count = 1);


    //! Increments the object's weak reference counter by one.
    /*!
     *  \returns the incremented counter.
     */
    int WeakRefObject();

    //! Decrements the object's weak reference counter by one.
    /*!
     *  \returns the decremented counter.
     */
    int WeakUnrefObject();


    //! Increments the object's import reference counter by one.
    /*!
     *  \returns the incremented counter.
     */
    int ImportRefObject();

    //! Decrements the object's import reference counter by one.
    /*!
     *  \returns the decremented counter.
     */
    int ImportUnrefObject();


    //! Sets weak reference counter to zero.
    void ResetWeakRefCounter();

    //! Returns the current reference counter.
    int GetObjectRefCounter() const;

    //! Returns the current weak reference counter.
    int GetObjectWeakRefCounter() const;

    //! Returns the current import reference counter.
    int GetImportRefCounter() const;

    //! Returns |true| iff the reference counter is positive.
    bool IsAlive() const;

    //! Returns |true| iff the type handler has destroyed the object and called #SetDestroyed.
    bool IsDestroyed() const;

    //! Returns |true| iff the weak ref counter is positive.
    bool IsLocked() const;

    //! Returns |true| iff the object is either non-versioned or versioned but does not belong to a transaction.
    bool IsTrunk() const;


    //! Returns an immutable collection of attributes associated with the object or |nullptr| is there are none.
    const TAttributeSet* GetAttributes() const;

    //! Returns (created if needed) a mutable collection of attributes associated with the object.
    TAttributeSet* GetMutableAttributes();

    //! Clears the collection of attributes associated with the object.
    void ClearAttributes();

protected:
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const TObjectId Id_;

    int RefCounter_ = 0;
    int WeakRefCounter_ = 0;
    int ImportRefCounter_ = 0;

    static constexpr int DestroyedRefCounter = -1;
    static constexpr int DisposedRefCounter = -2;
    
    std::unique_ptr<TAttributeSet> Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

TObjectId GetObjectId(const TObjectBase* object);
bool IsObjectAlive(const TObjectBase* object);

template <class T>
std::vector<TObjectId> ToObjectIds(
    const T& objects,
    size_t sizeLimit = std::numeric_limits<size_t>::max());

template <class TKey, class TValue, class THash>
std::vector<TValue*> GetValuesSortedByKey(const NHydra::TReadOnlyEntityMap<TKey, TValue, THash>& entities);

////////////////////////////////////////////////////////////////////////////////

class TNonversionedObjectBase
    : public TObjectBase
{
public:
    explicit TNonversionedObjectBase(const TObjectId& id);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

#define OBJECT_INL_H_
#include "object-inl.h"
#undef OBJECT_INL_H_

