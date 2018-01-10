#pragma once

#include "public.h"
#include "attribute_set.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/hydra/entity_map.h>

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TObjectDynamicData
    : public NHydra::TEntityDynamicDataBase
{ };

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for all objects in YT master server.
class TObjectBase
    : public NHydra::TEntityBase
{
public:
    explicit TObjectBase(const TObjectId& id);
    virtual ~TObjectBase();

    TObjectDynamicData* GetDynamicData() const;

    //! Marks the object as destroyed.
    void SetDestroyed();

    //! Marks the object as foreign.
    void SetForeign();

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
    int WeakRefObject(TEpoch epoch);

    //! Decrements the object's weak reference counter by one.
    /*!
     *  \returns the decremented counter.
     */
    int WeakUnrefObject(TEpoch epoch);


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


    //! Returns the current reference counter.
    int GetObjectRefCounter() const;

    //! Returns the current weak reference counter.
    int GetObjectWeakRefCounter(TEpoch epoch) const;

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

    //! Returns |true| if the object was replicated here from another cell.
    bool IsForeign() const;


    //! Returns an immutable collection of attributes associated with the object or |nullptr| is there are none.
    const TAttributeSet* GetAttributes() const;

    //! Returns (created if needed) a mutable collection of attributes associated with the object.
    TAttributeSet* GetMutableAttributes();

    //! Clears the collection of attributes associated with the object.
    void ClearAttributes();

    //! Returns the relative complexity of object destruction.
    //! This value must always be positive. The default is 10.
    virtual int GetGCWeight() const;

    //! Performs static cast to a given type.
    //! Note that we only use single inheritance inside TObject hierarchy.
    template <class TDerived>
    TDerived* As();
    template <class TDerived>
    const TDerived* As() const;

protected:
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const TObjectId Id_;

    int RefCounter_ = 0;
    int WeakRefCounter_ = 0;
    TEpoch WeakLockEpoch_ = 0;
    int ImportRefCounter_ = 0;

    struct {
        bool Foreign : 1;
        bool Destroyed : 1;
        bool Disposed : 1;
        bool Trunk : 1;
    } Flags_ = {};

    std::unique_ptr<TAttributeSet> Attributes_;

};

////////////////////////////////////////////////////////////////////////////////

struct TObjectRefComparer
{
    static bool Compare(const TObjectBase* lhs, const TObjectBase* rhs);
};

TObjectId GetObjectId(const TObjectBase* object);
bool IsObjectAlive(const TObjectBase* object);

template <class T>
std::vector<TObjectId> ToObjectIds(
    const T& objects,
    size_t sizeLimit = std::numeric_limits<size_t>::max());

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const NHydra::TReadOnlyEntityMap<TValue>& entities);

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const yhash_set<TValue*>& entities);

template <class TObject, class TValue>
std::vector<std::pair<TObject*, TValue>> GetPairsSortedByKey(const yhash<TObject*, TValue>& entities);

////////////////////////////////////////////////////////////////////////////////

class TNonversionedObjectBase
    : public TObjectBase
{
public:
    explicit TNonversionedObjectBase(const TObjectId& id);

};

////////////////////////////////////////////////////////////////////////////////

struct TObjectIdFormatter
{
    void operator()(TStringBuilder* builder, const TObjectBase* object) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

#define OBJECT_INL_H_
#include "object-inl.h"
#undef OBJECT_INL_H_

