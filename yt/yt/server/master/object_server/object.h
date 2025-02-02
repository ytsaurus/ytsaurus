#pragma once

#include "attribute_set.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/sequoia_server/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/lib/misc/assert_sizeof.h>

#include <yt/yt/core/misc/pool_allocator.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

struct TObjectDynamicData
    : public NHydra::TEntityDynamicDataBase
{ };

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TObjectDynamicData, 4);

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
    virtual std::string GetLowercaseObjectName() const;
    virtual std::string GetCapitalizedObjectName() const;

    //! Builds a human-readable path for diagnostics.
    virtual NYPath::TYPath GetObjectPath() const;

    //! Returns an immutable collection of attributes associated with the object or |nullptr| is there are none.
    const TAttributeSet* GetAttributes() const;

    //! Returns (created if needed) a mutable collection of attributes associated with the object.
    TAttributeSet* GetMutableAttributes();

    //! Clears the collection of attributes associated with the object.
    void ClearAttributes();

    //! Returns a pointer to the value of the attribute or |nullptr| if it is not set.
    const NYson::TYsonString* FindAttribute(const std::string& key) const;

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
    // COMPAT(cherepashka): remove after 25.1.
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

#ifdef YT_ROPSAN_ENABLE_PTR_TAGGING
    template <class T>
    friend class TRawObjectPtr;

    TRopSanTag RopSanTag_;

    static TRopSanTag GenerateRopSanTag();
#endif

    std::unique_ptr<TAttributeSet> Attributes_;
};

using TObjectRawPtr = ::NYT::NObjectServer::TRawObjectPtr<TObject>;

// Think twice before increasing this.
YT_STATIC_ASSERT_SIZEOF_SANITY(TObject, 88);

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

template <class TValuePtr>
std::vector<TValuePtr> GetValuesSortedByKey(const THashSet<TValuePtr>& entities);

template <typename TKey, class TValuePtr>
std::vector<TValuePtr> GetValuesSortedById(const THashMap<TKey, TValuePtr>& entities);

template <class TObjectPtr, class TValue>
std::vector<typename THashMap<TObjectPtr, TValue>::iterator> GetIteratorsSortedByKey(THashMap<TObjectPtr, TValue>& entities);

////////////////////////////////////////////////////////////////////////////////

struct TObjectIdFormatter
{
    void operator()(TStringBuilderBase* builder, const TObject* object) const;
};

////////////////////////////////////////////////////////////////////////////////

void InitializeMasterStateThread(
    NCellMaster::TBootstrap* bootstrap,
    TEpochContextPtr epochContext,
    bool isAutomatonThread);
void FinalizeMasterStateThread();

void BeginEpoch();
void EndEpoch();
TEpoch GetCurrentEpoch();

void BeginMutation();
void EndMutation();
bool IsInMutation();

void BeginTeardown();
void EndTeardown();

void FlushObjectUnrefs();

void AssertAutomatonThreadAffinity();
void VerifyAutomatonThreadAffinity();

void AssertPersistentStateRead();
void VerifyPersistentStateRead();

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
    bool operator==(TRawObjectPtr<U> other) const noexcept;
    template <class U>
    bool operator==(U* other) const noexcept;

private:
    T* Ptr_ = nullptr;
    [[no_unique_address]]
    C Context_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TRawObjectPtr
{
public:
    TRawObjectPtr() noexcept = default;
    TRawObjectPtr(const TRawObjectPtr& other) noexcept = default;
    TRawObjectPtr(TRawObjectPtr&& other) noexcept = default;
    // Intentionally implicit.
    TRawObjectPtr(T* ptr) noexcept;

    TRawObjectPtr& operator=(const TRawObjectPtr& other) = default;
    TRawObjectPtr& operator=(TRawObjectPtr&& other) = default;

    T* operator->() const noexcept;

    operator T*() const noexcept;

    template <class U>
        requires std::derived_from<T, U>
    operator TRawObjectPtr<U>() const noexcept;

    explicit operator bool() const noexcept;

    T* Get() const noexcept;

    //! Same as |Get| but does not check tags.
    T* GetUnsafe() const noexcept;

#ifdef YT_ROPSAN_ENABLE_PTR_TAGGING
    void VerifyRopSanTag() const noexcept;
#endif

private:
#ifdef YT_ROPSAN_ENABLE_PTR_TAGGING
    static constexpr int PtrBitWidth = 48;
    uintptr_t TaggedPtr_ = 0;

    static uintptr_t MakeTaggedPtr(T* ptr) noexcept;

    TRopSanTag GetTag() const noexcept;
#else
    T* Ptr_ = nullptr;
#endif
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
bool IsObjectAlive(const TObjectPtr<T, C>& ptr);

template <class T, class C>
TObjectId GetObjectId(const TObjectPtr<T, C>& ptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

#define OBJECT_INL_H_
#include "object-inl.h"
#undef OBJECT_INL_H_

