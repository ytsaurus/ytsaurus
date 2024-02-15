#pragma once

#include "attribute_set.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/sequoia_server/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/core/misc/pool_allocator.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TObjectDynamicData
    : public NHydra::TEntityDynamicDataBase
{ };

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
    int Increment(int delta);

    void Persist(const TStreamPersistenceContext& context);

private:
    int ShardIndex_;

    int Value_ = 0;
    TEpoch Epoch_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Provides a base for all objects in YT master server.
class TObject
    : public NHydra::TEntityBase
    , public TPoolAllocator::TObjectBase
{
public:
    //! For Sequoia objects equals to its aevum which is a version of representation of
    //! object in dynamic tables.
    //! For non-Sequoia objects equals to |EAevum::None|.
    DEFINE_BYVAL_RW_PROPERTY(NSequoiaServer::EAevum, Aevum, NSequoiaServer::EAevum::None);

    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, AttributeRevision, NHydra::NullRevision);
    DEFINE_BYVAL_RW_PROPERTY(NHydra::TRevision, ContentRevision, NHydra::NullRevision);

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

    //! Returns |true| if this is a Sequoia object.
    bool IsSequoia() const;


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
     *  |true| makes it exact but induces side effects and can only be used deterministically and in mutation.
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

    //! Builds a human-readable path for diagnostics.
    virtual TString GetObjectPath() const;

    //! Returns an immutable collection of attributes associated with the object or |nullptr| is there are none.
    const TAttributeSet* GetAttributes() const;

    //! Returns (created if needed) a mutable collection of attributes associated with the object.
    TAttributeSet* GetMutableAttributes();

    //! Clears the collection of attributes associated with the object.
    void ClearAttributes();

    //! Returns a pointer to the value of the attribute or |nullptr| if it is not set.
    const NYson::TYsonString* FindAttribute(const TString& key) const;

    //! For Sequoia objects sets aevum equal to the current aevum.
    //! For non-Sequoia objects does nothing.
    void RememberAevum();

    NHydra::TRevision GetRevision() const;
    virtual void SetModified(EModificationType modificationType);

    //! Returns the relative complexity of object destruction.
    //! This value must always be positive. The default is 10.
    virtual int GetGCWeight() const;

    //! Checks if object-specific invariants are held.
    virtual void CheckInvariants(NCellMaster::TBootstrap* bootstrap) const;

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
    // COMPAT(h0pless): Make this field const when schema migration will be finished.
    TObjectId Id_;

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

bool IsInMutation();

void BeginTeardown();
void EndTeardown();

void FlushObjectUnrefs();

////////////////////////////////////////////////////////////////////////////////

struct TObjectPtrLoadTag
{ };

template <class T, class C>
class TObjectPtr
{
public:
    TObjectPtr() noexcept = default;
    TObjectPtr(TObjectPtr&& other) noexcept;
    explicit TObjectPtr(T* ptr) noexcept;
    TObjectPtr(T* ptr, TObjectPtrLoadTag) noexcept;

    ~TObjectPtr() noexcept;

    TObjectPtr& operator=(TObjectPtr&& other) noexcept;

    // There are at least 3 reasons to make `TObjectPtr` not copyable:
    //  1. `auto foo = ...` - calls copy constructor, which increments possibly
    //     persistent reference counter. It's not allowed outside of mutation.
    //  2. `for (auto foo : ...)` - the same problem as previous one.
    //  3. Migration from raw pointers to smart ones with copy constructor and
    //     copy assignment operator is error-prone since `auto foo = ...` and
    //     `for (auto foo : ...)` are valid for raw pointers. Without
    //     compilation error here it can be detected only at runtime.
    TObjectPtr Clone() const noexcept;
    TObjectPtr(const TObjectPtr& other) = delete;
    const TObjectPtr& operator=(const TObjectPtr& other) = delete;

    void Assign(T* ptr) noexcept;
    void Assign(T* ptr, TObjectPtrLoadTag) noexcept;
    void Reset() noexcept;

    T* operator->() const noexcept;

    explicit operator bool() const noexcept;

    T* Get() const noexcept;

    //! Same as |Get| but does not check thread affinity.
    T* GetUnsafe() const noexcept;

    //! Same as |Reset| but does not check object liveness.
    void ResetOnClear() noexcept;

    template <class U>
    bool operator==(const TObjectPtr<U, C>& other) const noexcept;
    template <class U>
    bool operator==(U* other) const noexcept;

private:
    T* Ptr_ = nullptr;
    [[no_unique_address]]
    C Context_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
bool IsObjectAlive(const TObjectPtr<T, C>& ptr);

template <class T, class C>
TObjectId GetObjectId(const TObjectPtr<T, C>& ptr);

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

