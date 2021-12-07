#pragma once

#include "attribute_set.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/core/misc/pool_allocator.h>

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

struct TEpochContext
    : public TRefCounted
{
    TEpoch CurrentEpoch = 0;
    TEpoch CurrentEpochCounter = 0;
    IInvokerPtr EphemeralPtrUnrefInvoker;
};

DEFINE_REFCOUNTED_TYPE(TEpochContext)

////////////////////////////////////////////////////////////////////////////////

class TEpochRefCounter
{
public:
    explicit TEpochRefCounter(TObjectId id);

    int GetValue() const;
    int UpdateValue(int delta);

    void Persist(const TStreamPersistenceContext& context);

private:
    int ShardIndex_;

    int RefCounter_ = 0;
    TEpoch RefCounterEpoch_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for all objects in YT master server.
class TObject
    : public NHydra::TEntityBase
    , public TPoolAllocator::TObjectBase
{
public:
    explicit TObject(TObjectId id);
    virtual ~TObject();


    TObjectDynamicData* GetDynamicData() const;


    //! Marks the object as ghost.
    void SetGhost();

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
    int EphemeralRefObject();

    //! Decrements the object's ephemeral reference counter by one.
    /*!
     *  \returns the decremented counter.
     */
    int EphemeralUnrefObject();


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


    //! Returns the current strong reference counter.
    /*!
     *  \param flushUnrefs If |false| then the returned value may be inaccurate due to scheduled unrefs;
     *  |true| makes it exact but induces side effects and can only be used in mutation.
     */
    int GetObjectRefCounter(bool flushUnrefs = false) const;

    //! Returns the current ephemeral reference counter.
    int GetObjectEphemeralRefCounter() const;

    //! Returns the current weak reference counter.
    /*!
     *  \param flushUnrefs See #GetObjectRefCounter.
     */
    int GetObjectWeakRefCounter(bool flushUnrefs = false) const;

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

    //! Returns |true| iff the type handler has destroyed the object and the object has been recreated as ghost.
    bool IsGhost() const;

    //! Returns |true| iff the object destructor has already been executed.
    //! For debugging only; accessing such instances is UB.
    bool IsDisposed() const;

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

    template <class TImpl>
    static void RecreateAsGhost(TImpl* object);
    virtual void SaveEctoplasm(TStreamSaveContext& context) const;
    virtual void LoadEctoplasm(TStreamLoadContext& context);

protected:
    const TObjectId Id_;

    int RefCounter_ = 0;
    TEpochRefCounter EphemeralRefCounter_;
    int WeakRefCounter_ = 0;
    int ImportRefCounter_ = 0;
    i16 LifeStageVoteCount_ = 0; // how many secondary cells have confirmed the life stage
    EObjectLifeStage LifeStage_ = EObjectLifeStage::CreationCommitted;

    struct TFlags
    {
        bool Foreign : 1;
        bool Ghost : 1;
        bool Disposed : 1;
        bool Trunk : 1;
    };
    TFlags Flags_ = {};

    std::unique_ptr<TAttributeSet> Attributes_;
};

////////////////////////////////////////////////////////////////////////////////

struct TObjectIdComparer
{
    template <class TObjectPtr>
    bool operator()(const TObjectPtr& lhs, const TObjectPtr& rhs) const;

    template <class TObjectPtr>
    static bool Compare(const TObjectPtr& lhs, const TObjectPtr& rhs);
};

TObjectId GetObjectId(const TObject* object);
bool IsObjectAlive(const TObject* object);

template <class TObjectPtrs>
std::vector<TObjectId> ToObjectIds(
    const TObjectPtrs& objects,
    size_t sizeLimit = std::numeric_limits<size_t>::max());

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const NHydra::TReadOnlyEntityMap<TValue>& entities);

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const THashSet<TValue*>& entities);

template <typename TKey, class TValue>
std::vector<TValue*> GetValuesSortedById(const THashMap<TKey, TValue*>& entities);

template <class TObject, class TValue>
std::vector<typename THashMap<TObject*, TValue>::iterator> GetIteratorsSortedByKey(THashMap<TObject*, TValue>& entities);

////////////////////////////////////////////////////////////////////////////////

struct TObjectIdFormatter
{
    void operator()(TStringBuilderBase* builder, const TObject* object) const;
};

////////////////////////////////////////////////////////////////////////////////

void SetupMasterBootstrap(NCellMaster::TBootstrap* bootstrap);
void SetupAutomatonThread();

void SetupEpochContext(TEpochContextPtr epochContext);

void ResetAll();

void BeginEpoch();
void EndEpoch();
TEpoch GetCurrentEpoch();

void BeginMutation();
void EndMutation();

void BeginTeardown();
void EndTeardown();

void FlushObjectUnrefs();

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
class TObjectPtr
{
public:
    TObjectPtr() noexcept = default;
    TObjectPtr(const TObjectPtr& other) noexcept;
    TObjectPtr(TObjectPtr&& other) noexcept;
    explicit TObjectPtr(T* ptr) noexcept;

    ~TObjectPtr() noexcept;

    TObjectPtr& operator=(const TObjectPtr& other) noexcept;
    TObjectPtr& operator=(TObjectPtr&& other) noexcept;

    void Assign(T* ptr) noexcept;
    void AssignOnLoad(T* ptr) noexcept;
    void Reset() noexcept;

    T* operator->() const noexcept;

    explicit operator bool() const noexcept;
    bool IsAlive() const noexcept;

    T* Get() const noexcept;

    template <class U>
    bool operator==(const TObjectPtr<U, C>& other) const noexcept;
    template <class U>
    bool operator!=(const TObjectPtr<U, C>& other) const noexcept;

    template <class U>
    bool operator==(U* other) const noexcept;
    template <class U>
    bool operator!=(U* other) const noexcept;

private:
    T* Ptr_ = nullptr;
    [[no_unique_address]]
    C Context_;
};

////////////////////////////////////////////////////////////////////////////////

template <class>
struct TObjectPtrTraits
{ };

template <class T, class C>
struct TObjectPtrTraits<TObjectPtr<T, C>>
{
    using TUnderlying = T;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

#define OBJECT_INL_H_
#include "object-inl.h"
#undef OBJECT_INL_H_

