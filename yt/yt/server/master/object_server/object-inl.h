#ifndef OBJECT_INL_H_
#error "Direct inclusion of this file is not allowed, include object.h"
// For the sake of sane code completion.
#include "object.h"
#endif

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

inline TObject::TObject(TObjectId id)
    : Id_(id)
    , EphemeralRefCounter_(Id_)
{
    // This is reset to false in TCypressNode ctor for non-trunk nodes.
    Flags_.Trunk = true;
}

inline TObject::~TObject()
{
    // To make debugging easier.
    Flags_.Disposed = true;
}

inline TObjectDynamicData* TObject::GetDynamicData() const
{
    return GetTypedDynamicData<TObjectDynamicData>();
}

inline void TObject::SetGhost()
{
    YT_VERIFY(RefCounter_ == 0);
    Flags_.Ghost = true;
}

inline void TObject::SetForeign()
{
    Flags_.Foreign = true;
}

inline TObjectId TObject::GetId() const
{
    return Id_;
}

inline int TObject::RefObject()
{
    YT_VERIFY(RefCounter_ >= 0);
    return ++RefCounter_;
}

inline int TObject::UnrefObject(int count)
{
    YT_VERIFY(RefCounter_ >= count);
    return RefCounter_ -= count;
}

inline int TObject::EphemeralRefObject()
{
    YT_VERIFY(IsObjectAlive(this));
    return EphemeralRefCounter_.Increment(+1);
}

inline int TObject::EphemeralUnrefObject()
{
    return EphemeralRefCounter_.Increment(-1);
}

inline int TObject::WeakRefObject()
{
    YT_VERIFY(IsObjectAlive(this));
    YT_VERIFY(WeakRefCounter_ >= 0);

    return ++WeakRefCounter_;
}

inline int TObject::WeakUnrefObject()
{
    YT_VERIFY(WeakRefCounter_ > 0);
    return --WeakRefCounter_;
}

inline int TObject::ImportRefObject()
{
    return ++ImportRefCounter_;
}

inline int TObject::ImportUnrefObject()
{
    YT_VERIFY(ImportRefCounter_ > 0);
    return --ImportRefCounter_;
}

inline int TObject::GetObjectRefCounter(bool flushUnrefs) const
{
    if (flushUnrefs) {
        FlushObjectUnrefs();
    }
    return RefCounter_;
}

inline int TObject::GetObjectWeakRefCounter(bool flushUnrefs) const
{
    if (flushUnrefs) {
        FlushObjectUnrefs();
    }
    return WeakRefCounter_;
}

inline int TObject::GetImportRefCounter() const
{
    return ImportRefCounter_;
}

inline EObjectLifeStage TObject::GetLifeStage() const
{
    return LifeStage_;
}

inline void TObject::SetLifeStage(EObjectLifeStage lifeStage)
{
    LifeStage_ = lifeStage;
}

inline bool TObject::IsBeingCreated() const
{
    return
        LifeStage_ == EObjectLifeStage::CreationStarted ||
        LifeStage_ == EObjectLifeStage::CreationPreCommitted;
}

inline bool TObject::IsBeingRemoved() const
{
    return
        LifeStage_ == EObjectLifeStage::RemovalStarted ||
        LifeStage_ == EObjectLifeStage::RemovalPreCommitted ||
        LifeStage_ == EObjectLifeStage::RemovalCommitted;
}

inline bool TObject::IsGhost() const
{
    return Flags_.Ghost;
}

inline bool TObject::IsDisposed() const
{
    return Flags_.Disposed;
}

inline bool TObject::IsTrunk() const
{
    return Flags_.Trunk;
}

inline bool TObject::IsForeign() const
{
    return Flags_.Foreign;
}

inline bool TObject::IsNative() const
{
    return !IsForeign();
}

template <class TDerived>
TDerived* TObject::As()
{
    return static_cast<TDerived*>(this);
}

template <class TDerived>
const TDerived* TObject::As() const
{
    return static_cast<const TDerived*>(this);
}

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
void TObject::RecreateAsGhost(TImpl* object)
{
    // "1 KB should be enough for any ghost".
    static constexpr auto SerializedGhostBufferSize = 1_KB;
    std::array<char, SerializedGhostBufferSize> buffer;
    TMemoryOutput output(buffer.data(), buffer.size());

    {
        TStreamSaveContext context(&output);
        object->SaveEctoplasm(context);
    }

    auto id = object->TObject::GetId();
    object->~TObject();
    new (object) TImpl(id);

    {
        TMemoryInput input(buffer.data(), output.End() - buffer.data());
        TStreamLoadContext context(&input);
        object->LoadEctoplasm(context);
    }

    object->SetGhost();
}

////////////////////////////////////////////////////////////////////////////////

template <class TObjectPtr>
inline bool TObjectIdComparer::operator()(const TObjectPtr& lhs, const TObjectPtr& rhs) const
{
    return Compare(lhs, rhs);
}

template <class TObjectPtr>
inline bool TObjectIdComparer::Compare(const TObjectPtr& lhs, const TObjectPtr& rhs)
{
    return lhs->GetId() < rhs->GetId();
}

////////////////////////////////////////////////////////////////////////////////

inline TObjectId GetObjectId(const TObject* object)
{
    return object ? object->GetId() : NullObjectId;
}

inline bool IsObjectAlive(const TObject* object)
{
    return object && object->GetObjectRefCounter() > 0;
}

template <class TObjectPtrs>
std::vector<TObjectId> ToObjectIds(const TObjectPtrs& objects, size_t sizeLimit)
{
    std::vector<TObjectId> result;
    result.reserve(std::min(objects.size(), sizeLimit));
    for (auto* object : objects) {
        if (result.size() == sizeLimit)
            break;
        result.push_back(object->GetId());
    }
    return result;
}

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const NHydra::TReadOnlyEntityMap<TValue>& entities)
{
    std::vector<TValue*> values;
    values.reserve(entities.size());

    for (const auto& [key, entity] : entities) {
        if (IsObjectAlive(entity)) {
            values.push_back(entity);
        }
    }
    std::sort(values.begin(), values.end(), TObjectIdComparer());
    return values;
}

template <class TValue>
std::vector<TValue*> GetValuesSortedByKey(const THashSet<TValue*>& entities)
{
    std::vector<TValue*> values;
    values.reserve(entities.size());

    for (auto* object : entities) {
        if (IsObjectAlive(object)) {
            values.push_back(object);
        }
    }
    std::sort(values.begin(), values.end(), TObjectIdComparer());
    return values;
}

template <typename TKey, class TValue>
std::vector<TValue*> GetValuesSortedById(const THashMap<TKey, TValue*>& entities)
{
    std::vector<TValue*> values;
    values.reserve(entities.size());

    for (const auto& [_, object] : entities) {
        if (IsObjectAlive(object)) {
            values.push_back(object);
        }
    }
    std::sort(values.begin(), values.end(), TObjectIdComparer());
    return values;
}

template <class TObject, class TValue>
std::vector<typename THashMap<TObject*, TValue>::iterator> GetIteratorsSortedByKey(THashMap<TObject*, TValue>& entities)
{
    std::vector<typename THashMap<TObject*, TValue>::iterator> iterators;
    iterators.reserve(entities.size());

    for (auto it = entities.begin(); it != entities.end(); ++it) {
        if (IsObjectAlive(it->first)) {
            iterators.push_back(it);
        }
    }
    std::sort(iterators.begin(), iterators.end(), [] (auto lhs, auto rhs) {
        return TObjectIdComparer::Compare(lhs->first, rhs->first);
    });
    return iterators;
}

////////////////////////////////////////////////////////////////////////////////

struct TStrongObjectPtrContext
{
    static constexpr bool Persistent = true;

    static TStrongObjectPtrContext Capture()
    {
        return {};
    }

    void Ref(TObject* object);
    void Unref(TObject* object);
};

struct TWeakObjectPtrContext
{
    static constexpr bool Persistent = true;

    static TWeakObjectPtrContext Capture()
    {
        return {};
    }

    void Ref(TObject* object);
    void Unref(TObject* object);
};

struct TEphemeralObjectPtrContext
{
    IObjectManagerPtr ObjectManager;
    TEpoch Epoch;
    IInvokerPtr EphemeralPtrUnrefInvoker;

    static TEphemeralObjectPtrContext Capture();

    bool IsCurrent() const;

    void Ref(TObject* object);
    void Unref(TObject* object);

    template <class F>
    void SafeUnref(F&& func) const
    {
        if (IsCurrent()) {
            func();
        } else {
            EphemeralPtrUnrefInvoker->Invoke(BIND(std::move(func)));
        }
    }
};

namespace NDetail {

void VerifyAutomatonThreadAffinity();
void VerifyPersistentStateRead();

inline void AssertAutomatonThreadAffinity()
{
#ifndef NDEBUG
    VerifyAutomatonThreadAffinity();
#endif
}

inline void AssertPersistentStateRead()
{
#ifndef NDEBUG
    VerifyPersistentStateRead();
#endif
}

inline void AssertObjectValidOrNull(TObject* object)
{
    YT_ASSERT(!object || !object->IsDisposed());
}

} // namespace NDetail

template <class T, class C>
TObjectPtr<T, C>::TObjectPtr(TObjectPtr&& other) noexcept
    : Ptr_(other.Ptr_)
    , Context_(std::move(other.Context_))
{
    NDetail::AssertObjectValidOrNull(ToObject(Ptr_));
    other.Ptr_ = nullptr;
}

template <class T, class C>
TObjectPtr<T, C>::TObjectPtr(T* ptr) noexcept
    : Ptr_(ptr)
    , Context_(C::Capture())
{
    NDetail::AssertPersistentStateRead();
    NDetail::AssertObjectValidOrNull(ToObject(Ptr_));
    if (Ptr_) {
        Context_.Ref(ToObject(Ptr_));
    }
}

template <class T, class C>
TObjectPtr<T, C>::TObjectPtr(T* ptr, TObjectPtrLoadTag) noexcept
    : Ptr_(ptr)
    , Context_(C::Capture())
{
    static_assert(C::Persistent);
    NDetail::AssertAutomatonThreadAffinity();
    NDetail::AssertObjectValidOrNull(ToObject(Ptr_));
}

template <class T, class C>
TObjectPtr<T, C>::~TObjectPtr() noexcept
{
    // NB: Object may be invalid during Clear.
    if (IsInMutation()) {
        NDetail::AssertObjectValidOrNull(ToObject(Ptr_));
    }

    if (Ptr_) {
        Context_.Unref(ToObject(Ptr_));
    }
}

template <class T, class C>
TObjectPtr<T, C>& TObjectPtr<T, C>::operator=(TObjectPtr&& other) noexcept
{
    NDetail::AssertPersistentStateRead();
    if (this != &other) {
        NDetail::AssertObjectValidOrNull(ToObject(Ptr_));
        NDetail::AssertObjectValidOrNull(ToObject(other.Ptr_));
        if (Ptr_) {
            Context_.Unref(ToObject(Ptr_));
        }
        Ptr_ = other.Ptr_;
        other.Ptr_ = nullptr;
        Context_ = std::move(other.Context_);
    }
    return *this;
}

template <class T, class C>
TObjectPtr<T, C> TObjectPtr<T, C>::Clone() const noexcept
{
    TObjectPtr<T, C> cloned;
    cloned.Assign(Ptr_);
    return cloned;
}

template <class T, class C>
void TObjectPtr<T, C>::Assign(T* ptr) noexcept
{
    NDetail::AssertPersistentStateRead();
    NDetail::AssertObjectValidOrNull(ToObject(Ptr_));
    if (Ptr_) {
        Context_.Unref(ToObject(Ptr_));
    }
    Ptr_ = ptr;
    Context_ = C::Capture();
    NDetail::AssertObjectValidOrNull(ToObject(Ptr_));
    if (Ptr_) {
        Context_.Ref(ToObject(Ptr_));
    }
}

template <class T, class C>
void TObjectPtr<T, C>::Assign(T* ptr, TObjectPtrLoadTag) noexcept
{
    static_assert(C::Persistent);
    YT_VERIFY(!Ptr_);
    Ptr_ = ptr;
    Context_ = C::Capture();
    NDetail::AssertObjectValidOrNull(ToObject(Ptr_));
}

template <class T, class C>
void TObjectPtr<T, C>::Reset() noexcept
{
    NDetail::AssertPersistentStateRead();
    NDetail::AssertObjectValidOrNull(ToObject(Ptr_));
    if (Ptr_) {
        Context_.Unref(ToObject(Ptr_));
        Ptr_ = nullptr;
        Context_ = {};
    }
}

template <class T, class C>
T* TObjectPtr<T, C>::operator->() const noexcept
{
    return Get();
}

template <class T, class C>
TObjectPtr<T, C>::operator bool() const noexcept
{
    NDetail::AssertPersistentStateRead();
    NDetail::AssertObjectValidOrNull(ToObject(Ptr_));
    return Ptr_ != nullptr;
}

template <class T, class C>
T* TObjectPtr<T, C>::Get() const noexcept
{
    NDetail::AssertPersistentStateRead();
    NDetail::AssertObjectValidOrNull(ToObject(Ptr_));
    return Ptr_;
}

template <class T, class C>
T* TObjectPtr<T, C>::GetUnsafe() const noexcept
{
    return Ptr_;
}

template <class T, class C>
void TObjectPtr<T, C>::ResetOnClear() noexcept
{
    YT_ASSERT(!IsInMutation());

    Ptr_ = nullptr;
}

template <class T, class C>
template <class U>
bool TObjectPtr<T, C>::operator==(const TObjectPtr<U, C>& other) const noexcept
{
    return *this == other.Get();
}

template <class T, class C>
template <class U>
bool TObjectPtr<T, C>::operator==(U* other) const noexcept
{
    NDetail::AssertPersistentStateRead();
    NDetail::AssertObjectValidOrNull(ToObject(Ptr_));
    NDetail::AssertObjectValidOrNull(ToObject(other));
    return Ptr_ == other;
}

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
inline bool IsObjectAlive(const TObjectPtr<T, C>& ptr)
{
    return IsObjectAlive(ptr.Get());
}

template <class T, class C>
inline TObjectId GetObjectId(const TObjectPtr<T, C>& ptr)
{
    return GetObjectId(ptr.Get());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer

//! Hasher for TObjectPtr.
template <class T, class C>
struct THash<NYT::NObjectServer::TObjectPtr<T, C>>
{
    Y_FORCE_INLINE size_t operator () (const NYT::NObjectServer::TObjectPtr<T, C>& ptr) const
    {
        return THash<T*>()(ptr.Get());
    }

    Y_FORCE_INLINE size_t operator () (T* ptr) const
    {
        return THash<T*>()(ptr);
    }
};

//! Equality for TObjectPtr.
template <class T, class C>
struct TEqualTo<NYT::NObjectServer::TObjectPtr<T, C>>
{
    Y_FORCE_INLINE bool operator () (
        const NYT::NObjectServer::TObjectPtr<T, C>& lhs,
        const NYT::NObjectServer::TObjectPtr<T, C>& rhs) const
    {
        return lhs == rhs;
    }

    Y_FORCE_INLINE bool operator () (
        const NYT::NObjectServer::TObjectPtr<T, C>& lhs,
        T* rhs) const
    {
        return lhs == rhs;
    }
};
