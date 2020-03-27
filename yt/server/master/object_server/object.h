#pragma once

#include "attribute_set.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/lib/hydra/entity_map.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TObjectDynamicData
    : public NHydra::TEntityDynamicDataBase
{ };

////////////////////////////////////////////////////////////////////////////////

// Some objects must be created and removed atomically.
//
// Let's consider accounts. In the absence of an atomic commit, it's possible
// that some cell knows about an account, and some other cell doesn't. Then, the
// former cell sending a chunk requisition update to the latter will cause
// trouble.
//
// Removal also needs two-phase (and even more!) locking since otherwise a primary master
// is unable to command the destruction of an object to its secondaries without risking
// that some secondary still holds a reference to the object.
DEFINE_ENUM_WITH_UNDERLYING_TYPE(EObjectLifeStage, ui8,
     // Creation workflow
     ((CreationStarted)         (0))
     ((CreationPreCommitted)    (1))
     ((CreationCommitted)       (2))

     // Removal workflow
     ((RemovalStarted)          (3))
     ((RemovalPreCommitted)     (4))
     ((RemovalAwaitingCellsSync)(5))
     ((RemovalCommitted)        (6))
);

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for all objects in YT master server.
class TObject
    : public NHydra::TEntityBase
{
public:
    explicit TObject(TObjectId id);
    virtual ~TObject();

    TObjectDynamicData* GetDynamicData() const;

    //! Marks the object as destroyed.
    void SetDestroyed();

    //! Marks the object as foreign.
    void SetForeign();

    //! Returns the object id.
    TObjectId GetId() const;

    //! Returns the cell tag extracted from id.
    TCellTag GetNativeCellTag() const;

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


    //! Increments the object's ephemeral reference counter by one.
    /*!
     *  \returns the incremented counter.
     *
     *  Ephemeral reference counter is just like the weak reference counter
     *  except that it automatically resets (to zero) on epoch change.
     */
    int EphemeralRefObject(TEpoch epoch);

    //! Decrements the object's ephemeral reference counter by one.
    /*!
     *  \returns the decremented counter.
     */
    int EphemeralUnrefObject(TEpoch epoch);


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


    //! Returns the current reference counter.
    int GetObjectRefCounter() const;

    //! Returns the current ephemeral reference counter.
    int GetObjectEphemeralRefCounter(TEpoch epoch) const;

    //! Returns the current weak reference counter.
    int GetObjectWeakRefCounter() const;

    //! Returns the current import reference counter.
    int GetImportRefCounter() const;

    //! Returns the current life stage of the object.
    EObjectLifeStage GetLifeStage() const;

    //! Sets the object's life stage.
    void SetLifeStage(EObjectLifeStage lifeStage);

    //! Returns the life stage vote count.
    int GetLifeStageVoteCount() const;

    //! Resets the life stage vote count to zero.
    void ResetLifeStageVoteCount();

    //! Increases life stage vote count and returns the vote count.
    int IncrementLifeStageVoteCount();

    //! Returns |true| iff object's creation has started but hasn't been committed yet.
    bool IsBeingCreated() const;

    //! Returns |true| iff the reference counter is positive.
    bool IsAlive() const;

    //! Returns |true| iff object's removal has started.
    bool IsBeingRemoved() const;

    //! Returns |true| iff the type handler has destroyed the object and called #SetDestroyed.
    bool IsDestroyed() const;

    //! Returns |true| iff the object is either non-versioned or versioned but does not belong to a transaction.
    bool IsTrunk() const;

    //! Returns |true| if the object was replicated here from another cell.
    bool IsForeign() const;

    //! Returns |true| if the objects is not foreign.
    bool IsNative() const;

    //! Builds a human-readable string for diagnostics.
    virtual TString GetLowercaseObjectName() const;
    virtual TString GetCapitalizedObjectName() const;

    //! Returns an immutable collection of attributes associated with the object or |nullptr| is there are none.
    const TAttributeSet* GetAttributes() const;

    //! Returns (created if needed) a mutable collection of attributes associated with the object.
    TAttributeSet* GetMutableAttributes();

    //! Clears the collection of attributes associated with the object.
    void ClearAttributes();

    //! Returns a pointer to the value of the attribute or |nullptr| if it is not set.
    const NYson::TYsonString* FindAttribute(const TString& key) const;

    //! Returns the relative complexity of object destruction.
    //! This value must always be positive. The default is 10.
    virtual int GetGCWeight() const;

    //! Performs static cast to a given type.
    //! Note that we only use single inheritance inside TObject hierarchy.
    template <class TDerived>
    TDerived* As();
    template <class TDerived>
    const TDerived* As() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

protected:
    const TObjectId Id_;

    int RefCounter_ = 0;
    int EphemeralRefCounter_ = 0;
    TEpoch EphemeralLockEpoch_ = 0;
    int WeakRefCounter_ = 0;
    int ImportRefCounter_ = 0;
    ui16 LifeStageVoteCount_ = 0; // how many secondary cells have confirmed the life stage
    EObjectLifeStage LifeStage_ = EObjectLifeStage::CreationCommitted;

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
    static bool Compare(const TObject* lhs, const TObject* rhs);
};

TObjectId GetObjectId(const TObject* object);
bool IsObjectAlive(const TObject* object);

template <class T>
std::vector<TObjectId> ToObjectIds(
    const T& objects,
    size_t sizeLimit = std::numeric_limits<size_t>::max());

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const NHydra::TReadOnlyEntityMap<TValue>& entities);

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const THashSet<TValue*>& entities);

template <class TObject, class TValue>
std::vector<typename THashMap<TObject*, TValue>::iterator> GetIteratorsSortedByKey(THashMap<TObject*, TValue>& entities);

////////////////////////////////////////////////////////////////////////////////

class TNonversionedObjectBase
    : public TObject
{
public:
    explicit TNonversionedObjectBase(TObjectId id);

    //! Throws if the current life stage is not #EObjectLifeStage::CreationCommitted.
    void ValidateActiveLifeStage() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TObjectIdFormatter
{
    void operator()(TStringBuilderBase* builder, const TObject* object) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

#define OBJECT_INL_H_
#include "object-inl.h"
#undef OBJECT_INL_H_

