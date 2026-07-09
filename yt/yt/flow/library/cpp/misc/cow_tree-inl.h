#pragma once

#ifndef COW_TREE_H_
    #error "Direct inclusion of this file is not allowed, include cow_tree.h"
    // For the sake of sane code completion.
    #include "cow_tree.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
const TKey& TCowTreeTraits<TKey, TValue>::KeyFromKV(const value_type& kv)
{
    return kv.first;
}

template <class TKey, class TValue>
TValue& TCowTreeTraits<TKey, TValue>::ValueFromKV(const value_type& kv)
{
    return kv.second;
}

template <class TKey, class TValue>
TValue&& TCowTreeTraits<TKey, TValue>::ValueFromKV(value_type&& kv)
{
    return std::move(kv.second);
}

template <class TKey, class TValue>
typename TCowTreeTraits<TKey, TValue>::ref_value_type TCowTreeTraits<TKey, TValue>::KVFromWrapper(const TWrapper& kv)
{
    return std::pair<const TKey, TValue&>(kv.first, kv.second->GetValue());
}

template <class TKey, class TValue>
typename TCowTreeTraits<TKey, TValue>::const_ref_value_type TCowTreeTraits<TKey, TValue>::ConstKVFromWrapper(const TWrapper& kv)
{
    return std::pair<const TKey, const TValue&>(kv.first, kv.second->GetValue());
}

template <class TKey, class TValue>
typename TCowTreeTraits<TKey, TValue>::ptr_value_type TCowTreeTraits<TKey, TValue>::PtrFromWrapper(const TWrapper& kv)
{
    return std::pair<const TKey&, TValue&>(kv.first, kv.second->GetValue());
}

template <class TKey, class TValue>
typename TCowTreeTraits<TKey, TValue>::const_ptr_value_type TCowTreeTraits<TKey, TValue>::ConstPtrFromWrapper(const TWrapper& kv)
{
    return std::pair<const TKey&, const TValue&>(kv.first, kv.second->GetValue());
}

template <class TKey, class TValue>
const TKey& TCowTreeTraits<TKey, TValue>::KeyFromWrapper(const TWrapper& kv)
{
    return kv.first;
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey>
const TKey& TCowTreeTraits<TKey, void>::KeyFromKV(const value_type& kv)
{
    return kv;
}

template <class TKey>
typename TCowTreeTraits<TKey, void>::ref_value_type TCowTreeTraits<TKey, void>::KVFromWrapper(const TWrapper& wrapper)
{
    return wrapper;
}

template <class TKey>
typename TCowTreeTraits<TKey, void>::const_ref_value_type TCowTreeTraits<TKey, void>::ConstKVFromWrapper(const TWrapper& wrapper)
{
    return wrapper;
}

template <class TKey>
typename TCowTreeTraits<TKey, void>::ptr_value_type TCowTreeTraits<TKey, void>::PtrFromWrapper(const TWrapper& wrapper)
{
    return wrapper;
}

template <class TKey>
typename TCowTreeTraits<TKey, void>::const_ptr_value_type TCowTreeTraits<TKey, void>::ConstPtrFromWrapper(const TWrapper& wrapper)
{
    return wrapper;
}

template <class TKey>
const TKey& TCowTreeTraits<TKey, void>::KeyFromWrapper(const TWrapper& wrapper)
{
    return wrapper;
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

//! Custom atomic refcounter to check whether the reference to the given object is single.
class TMaybeInplaceRefCounted
    : public TRefCounted
{
    template <class T>
    friend class TMaybeInplaceIntrusivePtr;

private:
    std::atomic<size_t> RefCount_;
    void MaybeInplaceRef();
    void MaybeInplaceUnref();

    bool IsSingleRef() const;
};

////////////////////////////////////////////////////////////////////////////////

//! Counters used for testing only that count TMaybeIntrusivePtr's AcquireSafe SlowPaths (actual clones) and FastPaths (inplace modifications).
#ifdef COW_TREE_DEBUG__
struct TMaybeIntrusivePtrAcquireCounters
{
    static std::atomic<size_t> FastPath;
    static std::atomic<size_t> SlowPath;

    static void Reset();
};
#endif

////////////////////////////////////////////////////////////////////////////////

//! Extension of IntrusivePtr that provides custom AcquireSafe (either clones the object or returns the reference to the same object) logic.
template <class T>
class TMaybeInplaceIntrusivePtr
    : public TIntrusivePtr<T>
{
    template <class TResult, class... Ts>
    friend TMaybeInplaceIntrusivePtr<TResult> MaybeInplaceNew(Ts&&... args);

public:
    TMaybeInplaceIntrusivePtr(std::nullptr_t);
    TMaybeInplaceIntrusivePtr();

    TMaybeInplaceIntrusivePtr(const TMaybeInplaceIntrusivePtr& other);
    TMaybeInplaceIntrusivePtr(TMaybeInplaceIntrusivePtr&& other) noexcept;
    template <class TAny>
    TMaybeInplaceIntrusivePtr(const TMaybeInplaceIntrusivePtr<TAny>& other);
    template <class TAny>
    TMaybeInplaceIntrusivePtr(TMaybeInplaceIntrusivePtr<TAny>&& other);

    ~TMaybeInplaceIntrusivePtr();

    TMaybeInplaceIntrusivePtr& operator=(const TMaybeInplaceIntrusivePtr& other);
    TMaybeInplaceIntrusivePtr& operator=(TMaybeInplaceIntrusivePtr&& other) noexcept;
    template <class TAny>
    TMaybeInplaceIntrusivePtr& operator=(const TMaybeInplaceIntrusivePtr<TAny>& other);
    template <class TAny>
    TMaybeInplaceIntrusivePtr& operator=(TMaybeInplaceIntrusivePtr<TAny>&& other);

    TMaybeInplaceIntrusivePtr AcquireSafe(TCowTreeVersionContext& versionContext);
    TMaybeInplaceIntrusivePtr AcquireSafeUnversioned();

private:
    //! This constructor implies the object had just been created, thus its refcount is 1.
    template <class TAny>
    TMaybeInplaceIntrusivePtr(TIntrusivePtr<TAny>&& other);
};

template <class TResult, class... Ts>
TMaybeInplaceIntrusivePtr<TResult> MaybeInplaceNew(Ts&&... args);

////////////////////////////////////////////////////////////////////////////////

//! Array to store keys/children/values. Supports some operations that are handy for CowTree.
template <class TKey, size_t MaxSize, class TLess = std::less<TKey>>
class TCowTreeSmartArray
{
public:
    //! Get index corresponding to upper/lower-bound result on the given key (from 0 to Size()).
    template <class TAnyKey>
    int UpperBound(const TAnyKey& key) const;
    template <class TAnyKey>
    int LowerBound(const TAnyKey& key) const;

    //! Get index of the element (missing key is UB).
    template <class TAnyKey>
    int GetIndex(const TAnyKey& key) const;
    //! Get index of the element (missing key's index is -1).
    template <class TAnyKey>
    int FindIndex(const TAnyKey& key) const;

    template <class TAnyKey>
    bool Contains(const TAnyKey& key) const;

    //! Get index of the first element satisfying the |predicate| (if no element satisfy - returns Size()).
    template <class TPredicate>
    int FirstSatisfying(const TPredicate& predicate) const;
    //! Get index of the last element satisfying the |predicate| (if no element satisfy - returns -1).
    template <class TPredicate>
    int LastSatisfying(const TPredicate& predicate) const;

    template <std::convertible_to<TKey> TAnyKey>
    void InsertByIndex(int index, TAnyKey&& key);

    //! Returns the index of the inserted element.
    template <std::convertible_to<TKey> TAnyKey>
    int Insert(TAnyKey&& key);

    void EraseByIndex(int index);
    //! Returns the index of the Erased element.
    template <class TAnyKey>
    int Erase(const TAnyKey& key);

    size_t Size() const;

    TKey& operator[](size_t index);
    const TKey& operator[](size_t index) const;

    //! Moves the first element to the end of |other|.
    void GiveLeftTo(TCowTreeSmartArray& other);
    //! Moves the last element to the beginning of |other|.
    void GiveRightTo(TCowTreeSmartArray& other);

    //! Moves the elements of |other| to the end of this.
    void MergeFromRight(TCowTreeSmartArray& other);

    void PushBack(TKey&& other);
    TKey PopBack();

    //! Moves all the elements besides |leaveAtLeft| first to another instance and returns it.
    TCowTreeSmartArray Split(size_t leaveAtLeft);

private:
    std::array<TKey, MaxSize> Keys_{};
    //! Total size of suCowTree.
    ui16 Size_ = 0;
    TLess Less_;
};

////////////////////////////////////////////////////////////////////////////////

//! Simple wrapper around a value to make in maybeinplace-refcounted.
template <class TValue>
class TCowTreeValueWrapper
    : public TMaybeInplaceRefCounted
{
public:
    TCowTreeValueWrapper() = default;
    TCowTreeValueWrapper(TValue&& value);
    TCowTreeValueWrapper(const TValue& value);

    TValue& GetValue();
    TMaybeInplaceIntrusivePtr<TCowTreeValueWrapper> Clone();

private:
    TValue Value_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t BlockSize, class TLess>
class TCowTreeLeaf;

template <class TKey, class TValue, size_t BlockSize, class TLess>
struct TCowTreeCachedPosition
{
    TCowTreeLeaf<TKey, TValue, BlockSize, TLess>* Leaf;
    int Index;
    typename TCowTreeTraits<TKey, TValue>::TWrapper Wrapper;
    size_t Version;

    //! Checks whether the cached position is still valid (the same key is at the same position and no CoW operations had been performed on the given tree).
    //! It is important to note that due to possible move of keys between siblings IsValid can be false even in a CachedPosition that had just been returned.
    bool IsValid(const TCowTreeVersionContext& versionContext) const;

    bool operator==(const TCowTreeCachedPosition& other) const;
    bool operator!=(const TCowTreeCachedPosition& other) const;

    TKey GetKey() const;
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t BlockSize, class TLess>
class TCowTreeInnerNode;

template <class TKey, class TValue, size_t BlockSize, class TLess>
class TCowTreeNode
    : public TMaybeInplaceRefCounted
{
    friend class TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>;
    friend class TCowTreeLeaf<TKey, TValue, BlockSize, TLess>;
    friend class TCowTree<TKey, TValue, BlockSize, TLess>;
    template <class TOtherKey, class TOtherValue, size_t OtherK, class TOtherLess>
    friend struct TCowTreeCachedPosition;

public:
    using value_type = typename TCowTreeTraits<TKey, TValue>::value_type;
    using TWrapper = typename TCowTreeTraits<TKey, TValue>::TWrapper;

public:
    TCowTreeNode() = default;

    TCowTreeNode(bool isLeaf);
    TCowTreeNode(const TCowTreeNode& other);

    TMaybeInplaceIntrusivePtr<TCowTreeNode> Clone();

    template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue, bool OverrideValue>
    std::pair<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>, bool> Insert(TAnyKeyValue&& kv, TCowTreeVersionContext& versionContext);
    //! Returns the count (0/1) of the erased elements.
    size_t Erase(const TKey& key, TCowTreeVersionContext& versionContext);

    void GiveRightTo(TCowTreeNode& rightSibling);
    void GiveLeftTo(TCowTreeNode& leftSibling);

    void MergeFromRight(TCowTreeNode& rightSibling);

    size_t KeySize() const;
    //! Returns the first key in the suCowTree.
    const TKey& FirstKey() const;

    //! Splits the oversaturated node into two.
    TMaybeInplaceIntrusivePtr<TCowTreeNode> Split();

    //! YT_ASSERTs that all the invariants for the CowTree are satisfied.
    void Verify(std::pair<int, int>& leafDepth, int depth, const std::optional<TKey>& minKey = std::nullopt, const std::optional<TKey>& maxKey = std::nullopt);

    //! Find the key in the suCowTree.
    template <bool ForModify>
    std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> Find(const TKey& key, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext);

    //! Find the first/last key satisfying the |predicate| in the suCowTree.
    template <bool ForModify>
    std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> FirstSatisfying(const std::function<bool(const TKey&)>& predicate, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext);
    template <bool ForModify>
    std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> LastSatisfying(const std::function<bool(const TKey&)>& predicate, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext);

    size_t Size() const;

private:
    // We intentionally allocate one more item for temporary storage.
    TCowTreeSmartArray<TKey, BlockSize + 1, TLess> Keys_;
    bool IsLeaf_ = false;
};

////////////////////////////////////////////////////////////////////////////////

//! Non-leaf node.
template <class TKey, class TValue, size_t BlockSize, class TLess>
class TCowTreeInnerNode
    : public TCowTreeNode<TKey, TValue, BlockSize, TLess>
{
    friend class TCowTree<TKey, TValue, BlockSize, TLess>;

public:
    using value_type = typename TCowTreeTraits<TKey, TValue>::value_type;
    using TWrapper = typename TCowTreeTraits<TKey, TValue>::TWrapper;

public:
    TCowTreeInnerNode() = default;
    TCowTreeInnerNode(TMaybeInplaceIntrusivePtr<TCowTreeNode<TKey, TValue, BlockSize, TLess>> lhs, TMaybeInplaceIntrusivePtr<TCowTreeNode<TKey, TValue, BlockSize, TLess>> rhs);

    template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue, bool OverrideValue>
    std::pair<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>, bool> Insert(TAnyKeyValue&& kv, TCowTreeVersionContext& versionContext);
    //! Returns the count (0/1) of the erased elements.
    size_t Erase(const TKey& key, TCowTreeVersionContext& versionContext);

    void GiveRightTo(TCowTreeInnerNode& other);
    void GiveLeftTo(TCowTreeInnerNode& other);

    void MergeFromRight(TCowTreeInnerNode& other);

    //! Splits the oversaturated node into two.
    TMaybeInplaceIntrusivePtr<TCowTreeInnerNode> Split();

    //! YT_ASSERTs that all the invariants for the CowTree are satisfied.
    void Verify(std::pair<int, int>& leafDepth, int depth, const std::optional<TKey>& minKey = std::nullopt, const std::optional<TKey>& maxKey = std::nullopt);

    //! Find the key in the suCowTree.
    template <bool ForModify>
    std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> Find(const TKey& key, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext);

    //! Find the first/last key satisfying the |predicate| in the suCowTree.
    template <bool ForModify>
    std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> FirstSatisfying(const std::function<bool(const TKey&)>& predicate, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext);
    template <bool ForModify>
    std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> LastSatisfying(const std::function<bool(const TKey&)>& predicate, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext);

    size_t Size() const;

private:
    TCowTreeSmartArray<TMaybeInplaceIntrusivePtr<TCowTreeNode<TKey, TValue, BlockSize, TLess>>, BlockSize + 1> Children_;
    size_t Size_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t BlockSize, class TLess>
class TCowTreeLeaf
    : public TCowTreeNode<TKey, TValue, BlockSize, TLess>
{
    friend class TCowTree<TKey, TValue, BlockSize, TLess>;
    template <class TOtherKey, class TOtherValue, size_t OtherK, class TOtherLess>
    friend struct TCowTreeCachedPosition;

    static constexpr bool HasValue = !std::is_void_v<TValue>;

public:
    using value_type = typename TCowTreeTraits<TKey, TValue>::value_type;
    using TWrapper = typename TCowTreeTraits<TKey, TValue>::TWrapper;

public:
    TCowTreeLeaf();
    template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue>
    TCowTreeLeaf(TAnyKeyValue&& keyValue);

    template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue, bool OverrideValue>
    std::pair<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>, bool> Insert(TAnyKeyValue&& kv, TCowTreeVersionContext& versionContext);
    //! Returns the count (0/1) of the erased elements.
    size_t Erase(const TKey& key, TCowTreeVersionContext& versionContext);

    void GiveRightTo(TCowTreeLeaf& other);
    void GiveLeftTo(TCowTreeLeaf& other);

    void MergeFromRight(TCowTreeLeaf& other);

    //! Splits the oversaturated node into two.
    TMaybeInplaceIntrusivePtr<TCowTreeLeaf> Split();

    //! YT_ASSERTs that all the invariants for the CowTree are satisfied.
    void Verify(std::pair<int, int>& leafDepth, int depth, const std::optional<TKey>& minKey = std::nullopt, const std::optional<TKey>& maxKey = std::nullopt);

    //! Find the key in the suCowTree.
    template <bool ForModify>
    std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> Find(const TKey& key, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext);

    //! Find the first/last key satisfying the |predicate| in the suCowTree.
    template <bool ForModify>
    std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> FirstSatisfying(const std::function<bool(const TKey&)>& predicate, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext);
    template <bool ForModify>
    std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> LastSatisfying(const std::function<bool(const TKey&)>& predicate, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext);

    size_t Size() const;

private:
    typename std::conditional_t<HasValue, TCowTreeSmartArray<TMaybeInplaceIntrusivePtr<TCowTreeValueWrapper<TValue>>, BlockSize + 1>, int> Values_;
    template <bool ForModify>
    TWrapper GetWrapperByIndex(int index);
    template <bool ForModify>
    TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess> BuildCachedPosition(int index, const TCowTreeVersionContext& versionContext);
};

////////////////////////////////////////////////////////////////////////////////

#ifdef COW_TREE_DEBUG__
inline std::atomic<size_t> TMaybeIntrusivePtrAcquireCounters::FastPath = 0;
inline std::atomic<size_t> TMaybeIntrusivePtrAcquireCounters::SlowPath = 0;

inline void TMaybeIntrusivePtrAcquireCounters::Reset()
{
    FastPath = 0;
    SlowPath = 0;
}
#endif

////////////////////////////////////////////////////////////////////////////////

template <class T>
TMaybeInplaceIntrusivePtr<T>::TMaybeInplaceIntrusivePtr(std::nullptr_t)
    : TIntrusivePtr<T>(nullptr)
{ }

template <class T>
TMaybeInplaceIntrusivePtr<T>::TMaybeInplaceIntrusivePtr()
    : TIntrusivePtr<T>(nullptr)
{ }

template <class T>
TMaybeInplaceIntrusivePtr<T>::TMaybeInplaceIntrusivePtr(const TMaybeInplaceIntrusivePtr& other)
    : TIntrusivePtr<T>(other)
{
    if (*this) {
        (*this)->MaybeInplaceRef();
    }
}

template <class T>
TMaybeInplaceIntrusivePtr<T>::TMaybeInplaceIntrusivePtr(TMaybeInplaceIntrusivePtr&& other) noexcept
    : TIntrusivePtr<T>(std::move(other))
{ }

template <class T>
template <class TAny>
TMaybeInplaceIntrusivePtr<T>::TMaybeInplaceIntrusivePtr(const TMaybeInplaceIntrusivePtr<TAny>& other)
    : TIntrusivePtr<T>(other)
{
    if (*this) {
        (*this)->MaybeInplaceRef();
    }
}

template <class T>
template <class TAny>
TMaybeInplaceIntrusivePtr<T>::TMaybeInplaceIntrusivePtr(TMaybeInplaceIntrusivePtr<TAny>&& other)
    : TIntrusivePtr<T>(std::move(other))
{ }

template <class T>
template <class TAny>
TMaybeInplaceIntrusivePtr<T>::TMaybeInplaceIntrusivePtr(TIntrusivePtr<TAny>&& other)
    : TIntrusivePtr<T>(std::move(other))
{
    if (*this) {
        (*this)->MaybeInplaceRef();
    }
}

template <class T>
TMaybeInplaceIntrusivePtr<T>::~TMaybeInplaceIntrusivePtr()
{
    if (*this) {
        (*this)->MaybeInplaceUnref();
    }
}

template <class T>
TMaybeInplaceIntrusivePtr<T>& TMaybeInplaceIntrusivePtr<T>::operator=(const TMaybeInplaceIntrusivePtr& other)
{
    if (*this) {
        (*this)->MaybeInplaceUnref();
    }
    TIntrusivePtr<T>::operator=(other);
    if (*this) {
        (*this)->MaybeInplaceRef();
    }
    return *this;
}

template <class T>
template <class TAny>
TMaybeInplaceIntrusivePtr<T>& TMaybeInplaceIntrusivePtr<T>::operator=(const TMaybeInplaceIntrusivePtr<TAny>& other)
{
    if (*this) {
        (*this)->MaybeInplaceUnref();
    }
    TIntrusivePtr<T>::operator=(other);
    if (*this) {
        (*this)->MaybeInplaceRef();
    }
    return *this;
}

template <class T>
template <class TAny>
TMaybeInplaceIntrusivePtr<T>& TMaybeInplaceIntrusivePtr<T>::operator=(TMaybeInplaceIntrusivePtr<TAny>&& other)
{
    if (*this) {
        (*this)->MaybeInplaceUnref();
    }
    TIntrusivePtr<T>::operator=(std::move(other));
    return *this;
}

template <class T>
TMaybeInplaceIntrusivePtr<T>& TMaybeInplaceIntrusivePtr<T>::operator=(TMaybeInplaceIntrusivePtr&& other) noexcept
{
    if (*this) {
        (*this)->MaybeInplaceUnref();
    }
    TIntrusivePtr<T>::operator=(std::move(other));
    return *this;
}

template <class T>
TMaybeInplaceIntrusivePtr<T> TMaybeInplaceIntrusivePtr<T>::AcquireSafeUnversioned()
{
    if (!(*this)) {
        return nullptr;
    }
    if ((*this)->IsSingleRef()) {
#ifdef COW_TREE_DEBUG__
        TMaybeIntrusivePtrAcquireCounters::FastPath.fetch_add(1);
#endif
        return *this;
    }
#ifdef COW_TREE_DEBUG__
    TMaybeIntrusivePtrAcquireCounters::SlowPath.fetch_add(1);
#endif
    return (*this)->Clone();
}

template <class T>
TMaybeInplaceIntrusivePtr<T> TMaybeInplaceIntrusivePtr<T>::AcquireSafe(TCowTreeVersionContext& versionContext)
{
    versionContext.Increment();
    return AcquireSafeUnversioned();
}

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class... Ts>
TMaybeInplaceIntrusivePtr<TResult> MaybeInplaceNew(Ts&&... args)
{
    return TMaybeInplaceIntrusivePtr<TResult>(New<TResult>(std::forward<Ts>(args)...));
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, size_t MaxSize, class TLess>
template <class TAnyKey>
int TCowTreeSmartArray<TKey, MaxSize, TLess>::UpperBound(const TAnyKey& key) const
{
    return std::upper_bound(Keys_.begin(), Keys_.begin() + Size_, key, Less_) - Keys_.begin();
}

template <class TKey, size_t MaxSize, class TLess>
template <class TAnyKey>
int TCowTreeSmartArray<TKey, MaxSize, TLess>::LowerBound(const TAnyKey& key) const
{
    return std::lower_bound(Keys_.begin(), Keys_.begin() + Size_, key, Less_) - Keys_.begin();
}

template <class TKey, size_t MaxSize, class TLess>
template <class TAnyKey>
int TCowTreeSmartArray<TKey, MaxSize, TLess>::GetIndex(const TAnyKey& key) const
{
    int result = LowerBound(key);
    YT_ASSERT(result >= 0 && result < Size_);
    // Keys_[result] <= key.
    YT_ASSERT(!Less_(key, Keys_[result]));
    return result;
}

template <class TKey, size_t MaxSize, class TLess>
template <class TAnyKey>
int TCowTreeSmartArray<TKey, MaxSize, TLess>::FindIndex(const TAnyKey& key) const
{
    int result = std::lower_bound(Keys_.begin(), Keys_.begin() + Size_, key, Less_) - Keys_.begin();
    // Keys_[result] > key.
    if (result == Size_ || Less_(key, Keys_[result])) {
        return -1;
    }

    return result;
}

template <class TKey, size_t MaxSize, class TLess>
template <class TAnyKey>
bool TCowTreeSmartArray<TKey, MaxSize, TLess>::Contains(const TAnyKey& key) const
{
    return FindIndex(key) != -1;
}

template <class TKey, size_t MaxSize, class TLess>
template <class TPredicate>
int TCowTreeSmartArray<TKey, MaxSize, TLess>::FirstSatisfying(const TPredicate& predicate) const
{
    return std::ranges::upper_bound(Keys_.begin(), Keys_.begin() + Size_, false, {}, predicate) - Keys_.begin();
}

template <class TKey, size_t MaxSize, class TLess>
template <class TPredicate>
int TCowTreeSmartArray<TKey, MaxSize, TLess>::LastSatisfying(const TPredicate& predicate) const
{
    return std::ranges::upper_bound(Keys_.begin(), Keys_.begin() + Size_, false, {}, [&predicate] (const TKey& key) {
        return !predicate(key);
    }) - Keys_.begin() -
        1;
}

template <class TKey, size_t MaxSize, class TLess>
template <std::convertible_to<TKey> TAnyKey>
void TCowTreeSmartArray<TKey, MaxSize, TLess>::InsertByIndex(int index, TAnyKey&& key)
{
    YT_ASSERT(index >= 0 && index <= (ssize_t)Size_);
    YT_ASSERT(Size_ + 1 <= MaxSize);
    for (int i = Size_ - 1; i >= index; i--) {
        Keys_[i + 1] = std::move(Keys_[i]);
    }

    Size_++;
    Keys_[index] = std::forward<TAnyKey>(key);
}

template <class TKey, size_t MaxSize, class TLess>
template <std::convertible_to<TKey> TAnyKey>
int TCowTreeSmartArray<TKey, MaxSize, TLess>::Insert(TAnyKey&& key)
{
    int index = LowerBound(key);
    InsertByIndex(index, std::forward<TAnyKey>(key));
    return index;
}

template <class TKey, size_t MaxSize, class TLess>
void TCowTreeSmartArray<TKey, MaxSize, TLess>::EraseByIndex(int index)
{
    YT_ASSERT(index >= 0 && index < (ssize_t)Size_);
    Size_--;
    for (int i = index; i < (ssize_t)Size_; i++) {
        Keys_[i] = std::move(Keys_[i + 1]);
    }

    Keys_[Size_] = TKey();
}

template <class TKey, size_t MaxSize, class TLess>
template <class TAnyKey>
int TCowTreeSmartArray<TKey, MaxSize, TLess>::Erase(const TAnyKey& key)
{
    int index = FindIndex(key);
    if (index != -1) {
        EraseByIndex(index);
    }
    return index;
}

template <class TKey, size_t MaxSize, class TLess>
size_t TCowTreeSmartArray<TKey, MaxSize, TLess>::Size() const
{
    return Size_;
}

template <class TKey, size_t MaxSize, class TLess>
TKey& TCowTreeSmartArray<TKey, MaxSize, TLess>::operator[](size_t index)
{
    return Keys_[index];
}

template <class TKey, size_t MaxSize, class TLess>
const TKey& TCowTreeSmartArray<TKey, MaxSize, TLess>::operator[](size_t index) const
{
    return Keys_[index];
}

template <class TKey, size_t MaxSize, class TLess>
void TCowTreeSmartArray<TKey, MaxSize, TLess>::GiveLeftTo(TCowTreeSmartArray& other)
{
    auto key = std::move(Keys_[0]);
    EraseByIndex(0);
    other.InsertByIndex(other.Size(), std::move(key));
}

template <class TKey, size_t MaxSize, class TLess>
void TCowTreeSmartArray<TKey, MaxSize, TLess>::GiveRightTo(TCowTreeSmartArray& other)
{
    auto key = std::move(Keys_[Size() - 1]);
    EraseByIndex(Size() - 1);
    other.InsertByIndex(0, std::move(key));
}

template <class TKey, size_t MaxSize, class TLess>
void TCowTreeSmartArray<TKey, MaxSize, TLess>::MergeFromRight(TCowTreeSmartArray& other)
{
    for (int i = 0; i < (int)other.Size(); i++) {
        InsertByIndex(Size(), std::move(other[i]));
    }
    for (int i = 0; i < (int)other.Size(); i++) {
        other.PopBack();
    }
}

template <class TKey, size_t MaxSize, class TLess>
void TCowTreeSmartArray<TKey, MaxSize, TLess>::PushBack(TKey&& other)
{
    InsertByIndex(Size(), std::move(other));
}

template <class TKey, size_t MaxSize, class TLess>
TKey TCowTreeSmartArray<TKey, MaxSize, TLess>::PopBack()
{
    auto key = std::move(Keys_[Size() - 1]);
    EraseByIndex(Size() - 1);
    return key;
}

template <class TKey, size_t MaxSize, class TLess>
TCowTreeSmartArray<TKey, MaxSize, TLess> TCowTreeSmartArray<TKey, MaxSize, TLess>::Split(size_t leaveAtLeft)
{
    TCowTreeSmartArray other;
    while (Size() > leaveAtLeft) {
        GiveRightTo(other);
    }
    return other;
}

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TCowTreeValueWrapper<TValue>::TCowTreeValueWrapper(TValue&& value)
    : Value_(std::move(value))
{ }

template <class TValue>
TCowTreeValueWrapper<TValue>::TCowTreeValueWrapper(const TValue& value)
    : Value_(value)
{ }

template <class TValue>
TValue& TCowTreeValueWrapper<TValue>::GetValue()
{
    return Value_;
}

template <class TValue>
TMaybeInplaceIntrusivePtr<TCowTreeValueWrapper<TValue>> TCowTreeValueWrapper<TValue>::Clone()
{
    return MaybeInplaceNew<TCowTreeValueWrapper>(Value_);
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t BlockSize, class TLess>
bool TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>::IsValid(const TCowTreeVersionContext& versionContext) const
{
    auto less = TLess();
    //! If there is a version mismatch, CoW's had been performed, therefore the leaf might no longer be present in the actual version of the tree.
    if (Version != versionContext.GetVersion()) {
        return false;
    }

    if ((int)Leaf->KeySize() <= Index) {
        return false;
    }

    // GetKey() != Leaf->Keys_[Index].
    if (less(Leaf->Keys_[Index], GetKey()) || less(GetKey(), Leaf->Keys_[Index])) {
        return false;
    }

    return true;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
bool TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>::operator==(const TCowTreeCachedPosition& other) const
{
    TLess less;
    const auto& leftKey = TCowTreeTraits<TKey, TValue>::KeyFromWrapper(Wrapper);
    const auto& rightKey = TCowTreeTraits<TKey, TValue>::KeyFromWrapper(other.Wrapper);
    return !less(leftKey, rightKey) && !less(rightKey, leftKey);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
bool TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>::operator!=(const TCowTreeCachedPosition& other) const
{
    return !(*this == other);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
TKey TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>::GetKey() const
{
    return TCowTreeTraits<TKey, TValue>::KeyFromWrapper(Wrapper);
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t BlockSize, class TLess>
TCowTreeNode<TKey, TValue, BlockSize, TLess>::TCowTreeNode(bool isLeaf)
    : IsLeaf_(isLeaf)
{ }

template <class TKey, class TValue, size_t BlockSize, class TLess>
TCowTreeNode<TKey, TValue, BlockSize, TLess>::TCowTreeNode(const TCowTreeNode<TKey, TValue, BlockSize, TLess>& other)
    : Keys_(other.Keys_)
    , IsLeaf_(other.IsLeaf_)
{ }

template <class TKey, class TValue, size_t BlockSize, class TLess>
TMaybeInplaceIntrusivePtr<TCowTreeNode<TKey, TValue, BlockSize, TLess>> TCowTreeNode<TKey, TValue, BlockSize, TLess>::Clone()
{
    if (IsLeaf_) {
        return MaybeInplaceNew<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>>(*static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>*>(this));
    } else {
        return MaybeInplaceNew<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>>(*static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(this));
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue, bool OverrideValue>
std::pair<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>, bool> TCowTreeNode<TKey, TValue, BlockSize, TLess>::Insert(TAnyKeyValue&& kv, TCowTreeVersionContext& versionContext)
{
    if (IsLeaf_) {
        return static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>*>(this)->template Insert<TAnyKeyValue, OverrideValue>(std::forward<TAnyKeyValue>(kv), versionContext);
    } else {
        return static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(this)->template Insert<TAnyKeyValue, OverrideValue>(std::forward<TAnyKeyValue>(kv), versionContext);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
size_t TCowTreeNode<TKey, TValue, BlockSize, TLess>::Erase(const TKey& key, TCowTreeVersionContext& versionContext)
{
    if (IsLeaf_) {
        return static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>*>(this)->Erase(key, versionContext);
    } else {
        return static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(this)->Erase(key, versionContext);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTreeNode<TKey, TValue, BlockSize, TLess>::GiveRightTo(TCowTreeNode<TKey, TValue, BlockSize, TLess>& other)
{
    YT_ASSERT(IsLeaf_ == other.IsLeaf_);

    Keys_.GiveRightTo(other.Keys_);

    if (IsLeaf_) {
        static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>*>(this)->GiveRightTo(static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>&>(other));
    } else {
        static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(this)->GiveRightTo(static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>&>(other));
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTreeNode<TKey, TValue, BlockSize, TLess>::GiveLeftTo(TCowTreeNode<TKey, TValue, BlockSize, TLess>& other)
{
    YT_ASSERT(IsLeaf_ == other.IsLeaf_);

    Keys_.GiveLeftTo(other.Keys_);

    if (IsLeaf_) {
        static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>*>(this)->GiveLeftTo(static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>&>(other));
    } else {
        static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(this)->GiveLeftTo(static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>&>(other));
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTreeNode<TKey, TValue, BlockSize, TLess>::MergeFromRight(TCowTreeNode<TKey, TValue, BlockSize, TLess>& other)
{
    YT_ASSERT(IsLeaf_ == other.IsLeaf_);

    Keys_.MergeFromRight(other.Keys_);

    if (IsLeaf_) {
        static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>*>(this)->MergeFromRight(static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>&>(other));
    } else {
        static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(this)->MergeFromRight(static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>&>(other));
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
size_t TCowTreeNode<TKey, TValue, BlockSize, TLess>::KeySize() const
{
    return Keys_.Size();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
const TKey& TCowTreeNode<TKey, TValue, BlockSize, TLess>::FirstKey() const
{
    YT_ASSERT(Keys_.Size() > 0);
    return Keys_[0];
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
TMaybeInplaceIntrusivePtr<TCowTreeNode<TKey, TValue, BlockSize, TLess>> TCowTreeNode<TKey, TValue, BlockSize, TLess>::Split()
{
    YT_ASSERT(Keys_.Size() == BlockSize + 1);

    auto rightKeys = Keys_.Split(BlockSize / 2 + 1);

    TMaybeInplaceIntrusivePtr<TCowTreeNode<TKey, TValue, BlockSize, TLess>> rightNode;
    if (IsLeaf_) {
        rightNode = static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>*>(this)->Split();
    } else {
        rightNode = static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(this)->Split();
    }

    rightNode->Keys_ = std::move(rightKeys);
    return rightNode;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTreeNode<TKey, TValue, BlockSize, TLess>::Verify(std::pair<int, int>& leafDepth, int depth, const std::optional<TKey>& minKey, const std::optional<TKey>& maxKey)
{
    YT_ASSERT(depth == 0 || KeySize() >= BlockSize / 2);
    YT_ASSERT(KeySize() <= BlockSize);

    auto less = TLess();
    for (int i = 0; i < (ssize_t)Keys_.Size() - 1; ++i) {
        YT_ASSERT(less(Keys_[i], Keys_[i + 1]));
    }

    if (minKey) {
        YT_ASSERT(!less(Keys_[0], *minKey));
    }
    if (maxKey) {
        YT_ASSERT(less(Keys_[Keys_.Size() - 1], *maxKey));
    }

    if (IsLeaf_) {
        static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>*>(this)->Verify(leafDepth, depth, minKey, maxKey);
    } else {
        static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(this)->Verify(leafDepth, depth, minKey, maxKey);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> TCowTreeNode<TKey, TValue, BlockSize, TLess>::Find(const TKey& key, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext)
{
    if (IsLeaf_) {
        return static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>*>(this)->template Find<ForModify>(key, versionContext);
    } else {
        return static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(this)->template Find<ForModify>(key, versionContext);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> TCowTreeNode<TKey, TValue, BlockSize, TLess>::FirstSatisfying(const std::function<bool(const TKey&)>& predicate, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext)
{
    if (IsLeaf_) {
        return static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>*>(this)->template FirstSatisfying<ForModify>(predicate, versionContext);
    } else {
        return static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(this)->template FirstSatisfying<ForModify>(predicate, versionContext);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> TCowTreeNode<TKey, TValue, BlockSize, TLess>::LastSatisfying(const std::function<bool(const TKey&)>& predicate, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext)
{
    if (IsLeaf_) {
        return static_cast<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>*>(this)->template LastSatisfying<ForModify>(predicate, versionContext);
    } else {
        return static_cast<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(this)->template LastSatisfying<ForModify>(predicate, versionContext);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
size_t TCowTreeNode<TKey, TValue, BlockSize, TLess>::Size() const
{
    if (IsLeaf_) {
        return static_cast<const TCowTreeLeaf<TKey, TValue, BlockSize, TLess>*>(this)->Size();
    } else {
        return static_cast<const TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(this)->Size();
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t BlockSize, class TLess>
TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>::TCowTreeInnerNode(TMaybeInplaceIntrusivePtr<TCowTreeNode<TKey, TValue, BlockSize, TLess>> lhs, TMaybeInplaceIntrusivePtr<TCowTreeNode<TKey, TValue, BlockSize, TLess>> rhs)
{
    this->Keys_.InsertByIndex(0, lhs->FirstKey());
    this->Keys_.InsertByIndex(1, rhs->FirstKey());
    Size_ = lhs->Size() + rhs->Size();
    Children_.InsertByIndex(0, std::move(lhs));
    Children_.InsertByIndex(1, std::move(rhs));
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue, bool OverrideValue>
std::pair<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>, bool> TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>::Insert(TAnyKeyValue&& keyValue, TCowTreeVersionContext& versionContext)
{
    int index = this->Keys_.UpperBound(TCowTreeTraits<TKey, TValue>::KeyFromKV(keyValue)) - 1;
    if (index < 0) {
        index = 0;
    }

    Children_[index] = Children_[index].AcquireSafe(versionContext);
    auto&& result = Children_[index]->template Insert<TAnyKeyValue, OverrideValue>(std::forward<TAnyKeyValue>(keyValue), versionContext);
    Size_ += result.second;
    this->Keys_[index] = Children_[index]->FirstKey();

    if (Children_[index]->KeySize() <= BlockSize) {
        return result;
    }

    YT_ASSERT(Children_[index]->KeySize() == BlockSize + 1);
    auto rightNode = Children_[index]->Split();
    this->Keys_.InsertByIndex(index + 1, rightNode->FirstKey());
    Children_.InsertByIndex(index + 1, std::move(rightNode));
    return result;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
size_t TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>::Erase(const TKey& key, TCowTreeVersionContext& versionContext)
{
    int index = this->Keys_.UpperBound(key) - 1;
    if (index < 0) {
        index = 0;
    }

    Children_[index] = Children_[index].AcquireSafe(versionContext);
    auto result = Children_[index]->Erase(key, versionContext);
    Size_ -= result;

    int childrenCount = Children_.Size();
    if (Children_[index]->KeySize() >= BlockSize / 2) {
        this->Keys_[index] = Children_[index]->FirstKey();
        return result;
    }

    YT_ASSERT(Children_[index]->KeySize() == BlockSize / 2 - 1);

    auto& child = Children_[index];
    if (index != 0 && Children_[index - 1]->KeySize() > BlockSize / 2) {
        // Move one key from child's left sibling to child.
        Children_[index - 1] = Children_[index - 1].AcquireSafe(versionContext);
        Children_[index - 1]->GiveRightTo(*child);
        this->Keys_[index] = Children_[index]->FirstKey();
    } else if (index != childrenCount - 1 && Children_[index + 1]->KeySize() > BlockSize / 2) {
        // Move one key from child's right sibling to child.
        Children_[index + 1] = Children_[index + 1].AcquireSafe(versionContext);
        Children_[index + 1]->GiveLeftTo(*child);
        this->Keys_[index + 1] = Children_[index + 1]->FirstKey();
        this->Keys_[index] = Children_[index]->FirstKey();
    } else if (index != 0) {
        // Merge child into its left sibling.
        Children_[index - 1] = Children_[index - 1].AcquireSafe(versionContext);
        Children_[index - 1]->MergeFromRight(*child);
        Children_.EraseByIndex(index);
        this->Keys_.EraseByIndex(index);
        this->Keys_[index - 1] = Children_[index - 1]->FirstKey();
    } else {
        // Merge child's right sibling into child.
        Children_[index + 1] = Children_[index + 1].AcquireSafe(versionContext);
        child->MergeFromRight(*Children_[index + 1]);
        Children_.EraseByIndex(index + 1);
        this->Keys_.EraseByIndex(index + 1);
        this->Keys_[index] = Children_[index]->FirstKey();
    }
    return result;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>::GiveRightTo(TCowTreeInnerNode& other)
{
    Children_.GiveRightTo(other.Children_);
    Size_ -= other.Children_[0]->Size();
    other.Size_ += other.Children_[0]->Size();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>::GiveLeftTo(TCowTreeInnerNode& other)
{
    Size_ -= Children_[0]->Size();
    other.Size_ += Children_[0]->Size();
    Children_.GiveLeftTo(other.Children_);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>::MergeFromRight(TCowTreeInnerNode& other)
{
    for (size_t i = 0; i < other.Children_.Size(); ++i) {
        Size_ += other.Children_[i]->Size();
    }
    Children_.MergeFromRight(other.Children_);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
TMaybeInplaceIntrusivePtr<TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>> TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>::Split()
{
    YT_ASSERT(Children_.Size() == BlockSize + 1);
    auto rightChildren = Children_.Split(BlockSize / 2 + 1);
    size_t rightSize = 0;
    for (size_t i = 0; i < rightChildren.Size(); ++i) {
        rightSize += rightChildren[i]->Size();
    }
    Size_ -= rightSize;
    TMaybeInplaceIntrusivePtr<TCowTreeInnerNode> rightNode = MaybeInplaceNew<TCowTreeInnerNode>();
    rightNode->Children_ = std::move(rightChildren);
    rightNode->Size_ = rightSize;
    return rightNode;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>::Verify(std::pair<int, int>& leafDepth, int depth, const std::optional<TKey>& minKey, const std::optional<TKey>& maxKey)
{
    auto less = TLess();
    YT_ASSERT(Children_.Size() <= BlockSize);
    YT_ASSERT(this->Keys_.Size() == Children_.Size());

    for (int i = 0; i < (ssize_t)Children_.Size(); ++i) {
        auto effectiveMinKey = minKey;
        effectiveMinKey = this->Keys_[i];

        auto effectiveMaxKey = maxKey;
        if (i != (ssize_t)Children_.Size() - 1) {
            effectiveMaxKey = this->Keys_[i + 1];
        }

        YT_ASSERT(!less(this->Keys_[i], Children_[i]->FirstKey()) && !less(Children_[i]->FirstKey(), this->Keys_[i]));
        Children_[i]->Verify(leafDepth, depth + 1, effectiveMinKey, effectiveMaxKey);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>::Find(const TKey& key, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext)
{
    int index = this->Keys_.UpperBound(key) - 1;
    if (index < 0) {
        return {};
    }
    if constexpr (ForModify) {
        Children_[index] = Children_[index].AcquireSafe(versionContext);
    }
    return Children_[index]->template Find<ForModify>(key, versionContext);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>::FirstSatisfying(const std::function<bool(const TKey&)>& predicate, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext)
{
    int index = this->Keys_.FirstSatisfying(predicate);
    if (index == 0) {
        // If index is 0, first key overall surely satisfies, we just need to descend to it.
        if constexpr (ForModify) {
            Children_[0] = Children_[0].AcquireSafe(versionContext);
        }
        return this->Children_[0]->template FirstSatisfying<ForModify>(predicate, versionContext);
    }
    // If the key satisfies, we still got to check the previous section since keys in there might still satisfy.
    if constexpr (ForModify) {
        Children_[index - 1] = Children_[index - 1].AcquireSafe(versionContext);
    }
    auto result = this->Children_[index - 1]->template FirstSatisfying<ForModify>(predicate, versionContext);
    if (!result) {
        if (index == (int)this->Keys_.Size()) {
            return {};
        }
        // If we didn't find the result in the "index - 1" section, we need to descend to the first key in the "index" section.
        // Thus, it is guaranteed that we will only pass through "if (index == 0)" blocks, thus, not worsening complexity.
        if constexpr (ForModify) {
            Children_[index] = Children_[index].AcquireSafe(versionContext);
        }
        return this->Children_[index]->template FirstSatisfying<ForModify>(predicate, versionContext);
    }
    return result;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>::LastSatisfying(const std::function<bool(const TKey&)>& predicate, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext)
{
    int index = this->Keys_.LastSatisfying(predicate);
    if (index == -1) {
        return {};
    }
    // Much easier than FirstSatisfying, since result is always in the same child as the key that satisfies.
    if constexpr (ForModify) {
        Children_[index] = Children_[index].AcquireSafe(versionContext);
    }
    return this->Children_[index]->template LastSatisfying<ForModify>(predicate, versionContext);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
size_t TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>::Size() const
{
    return Size_;
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t BlockSize, class TLess>
TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::TCowTreeLeaf()
    : TCowTreeNode<TKey, TValue, BlockSize, TLess>(true)
{ }

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue>
TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::TCowTreeLeaf(TAnyKeyValue&& keyValue)
    : TCowTreeLeaf()
{
    this->Keys_.InsertByIndex(0, TCowTreeTraits<TKey, TValue>::KeyFromKV(keyValue));
    if constexpr (HasValue) {
        YT_ASSERT(Values_.Size() == 0);
        Values_.InsertByIndex(0, MaybeInplaceNew<TCowTreeValueWrapper<TValue>>(TCowTreeTraits<TKey, TValue>::ValueFromKV(std::forward<TAnyKeyValue>(keyValue))));
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue, bool OverrideValue>
std::pair<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>, bool> TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::Insert(TAnyKeyValue&& keyValue, TCowTreeVersionContext& versionContext)
{
    int index = this->Keys_.FindIndex(TCowTreeTraits<TKey, TValue>::KeyFromKV(keyValue));
    if (index != -1) {
        return {BuildCachedPosition<OverrideValue>(index, versionContext), false};
    }
    int insertIndex = this->Keys_.Insert(TCowTreeTraits<TKey, TValue>::KeyFromKV(keyValue));
    if constexpr (HasValue) {
        Values_.InsertByIndex(insertIndex, MaybeInplaceNew<TCowTreeValueWrapper<TValue>>(TCowTreeTraits<TKey, TValue>::ValueFromKV(std::forward<TAnyKeyValue>(keyValue))));
    }
    return {BuildCachedPosition<false>(insertIndex, versionContext), true};
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
size_t TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::Erase(const TKey& key, TCowTreeVersionContext&)
{
    int index = this->Keys_.Erase(key);
    if (index != -1) {
        if constexpr (HasValue) {
            Values_.EraseByIndex(index);
        }
    }
    return (index == -1) ? 0 : 1;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::GiveRightTo(TCowTreeLeaf<TKey, TValue, BlockSize, TLess>& other)
{
    if constexpr (HasValue) {
        Values_.GiveRightTo(other.Values_);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::GiveLeftTo(TCowTreeLeaf<TKey, TValue, BlockSize, TLess>& other)
{
    if constexpr (HasValue) {
        Values_.GiveLeftTo(other.Values_);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::MergeFromRight(TCowTreeLeaf<TKey, TValue, BlockSize, TLess>& other)
{
    if constexpr (HasValue) {
        Values_.MergeFromRight(other.Values_);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
TMaybeInplaceIntrusivePtr<TCowTreeLeaf<TKey, TValue, BlockSize, TLess>> TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::Split()
{
    TMaybeInplaceIntrusivePtr<TCowTreeLeaf> rightNode = MaybeInplaceNew<TCowTreeLeaf>();
    if constexpr (HasValue) {
        YT_ASSERT(Values_.Size() == BlockSize + 1);
        auto rightValues = Values_.Split(BlockSize / 2 + 1);
        rightNode->Values_ = std::move(rightValues);
    }
    return rightNode;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::Verify(std::pair<int, int>& leafDepth, int depth, const std::optional<TKey>&, const std::optional<TKey>&)
{
    leafDepth.first = std::min(leafDepth.first, depth);
    leafDepth.second = std::max(leafDepth.second, depth);

    if constexpr (HasValue) {
        YT_ASSERT(Values_.Size() == this->KeySize());
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::Find(const TKey& key, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext)
{
    int index = this->Keys_.FindIndex(key);
    if (index == -1) {
        return {};
    }
    return {BuildCachedPosition<ForModify>(index, versionContext)};
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::FirstSatisfying(const std::function<bool(const TKey&)>& predicate, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext)
{
    int index = this->Keys_.FirstSatisfying(predicate);
    if (index == (int)this->KeySize()) {
        return {};
    }
    return {BuildCachedPosition<ForModify>(index, versionContext)};
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
std::optional<TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::LastSatisfying(const std::function<bool(const TKey&)>& predicate, std::conditional_t<ForModify, TCowTreeVersionContext&, const TCowTreeVersionContext&> versionContext)
{
    int index = this->Keys_.LastSatisfying(predicate);
    if (index == -1) {
        return {};
    }
    return {BuildCachedPosition<ForModify>(index, versionContext)};
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
size_t TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::Size() const
{
    return this->KeySize();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
typename TCowTreeTraits<TKey, TValue>::TWrapper TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::GetWrapperByIndex(int index)
{
    if constexpr (HasValue) {
        if constexpr (ForModify) {
            Values_[index] = Values_[index].AcquireSafeUnversioned();
        }
        return {this->Keys_[index], Values_[index].Get()};
    } else {
        return this->Keys_[index];
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess> TCowTreeLeaf<TKey, TValue, BlockSize, TLess>::BuildCachedPosition(int index, const TCowTreeVersionContext& context)
{
    return {this, index, GetWrapperByIndex<ForModify>(index), context.GetVersion()};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t BlockSize, class TLess>
TCowTree<TKey, TValue, BlockSize, TLess>::TNodeHandle::TNodeHandle(TCowTree<TKey, TValue, BlockSize, TLess>::value_type&& keyValue)
    : Value_(std::move(keyValue))
{ }

template <class TKey, class TValue, size_t BlockSize, class TLess>
const typename TCowTree<TKey, TValue, BlockSize, TLess>::value_type& TCowTree<TKey, TValue, BlockSize, TLess>::TNodeHandle::value() const&
{
    return Value_.value();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::value_type& TCowTree<TKey, TValue, BlockSize, TLess>::TNodeHandle::value() &
{
    return Value_.value();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::value_type&& TCowTree<TKey, TValue, BlockSize, TLess>::TNodeHandle::value() &&
{
    return std::move(Value_).value();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
bool TCowTree<TKey, TValue, BlockSize, TLess>::TNodeHandle::empty() const
{
    return !Value_.has_value();
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool Modifiable>
TCowTree<TKey, TValue, BlockSize, TLess>::TIterator<Modifiable>::TIterator(std::optional<NDetail::TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> wrapper, const TCowTree* tree)
    : CachedPosition_(wrapper)
    , Tree_(tree)
{ }

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool Modifiable>
TCowTree<TKey, TValue, BlockSize, TLess>::TIterator<Modifiable>::TIterator()
    : TIterator(std::nullopt, nullptr)
{ }

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool Modifiable>
typename TCowTree<TKey, TValue, BlockSize, TLess>::template TIterator<Modifiable>::reference TCowTree<TKey, TValue, BlockSize, TLess>::TIterator<Modifiable>::operator*() const
{
    if constexpr (Modifiable) {
        return TCowTreeTraits<TKey, TValue>::KVFromWrapper(CachedPosition_->Wrapper);
    } else {
        return TCowTreeTraits<TKey, TValue>::ConstKVFromWrapper(CachedPosition_->Wrapper);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool Modifiable>
typename TCowTree<TKey, TValue, BlockSize, TLess>::template TIterator<Modifiable>::pointer TCowTree<TKey, TValue, BlockSize, TLess>::TIterator<Modifiable>::operator->() const
{
    if constexpr (Modifiable) {
        return TCowTreeTraits<TKey, TValue>::PtrFromWrapper(CachedPosition_->Wrapper);
    } else {
        return TCowTreeTraits<TKey, TValue>::ConstPtrFromWrapper(CachedPosition_->Wrapper);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool Modifiable>
bool TCowTree<TKey, TValue, BlockSize, TLess>::TIterator<Modifiable>::operator==(const TIterator& other) const
{
    return Tree_ == other.Tree_ && CachedPosition_ == other.CachedPosition_;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool Modifiable>
bool TCowTree<TKey, TValue, BlockSize, TLess>::TIterator<Modifiable>::operator!=(const TIterator& other) const
{
    return !(*this == other);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool Modifiable>
typename TCowTree<TKey, TValue, BlockSize, TLess>::template TIterator<Modifiable>& TCowTree<TKey, TValue, BlockSize, TLess>::TIterator<Modifiable>::operator++()
{
    if (CachedPosition_->IsValid(Tree_->VersionContext_)) {
        auto& leaf = CachedPosition_->Leaf;
        int& index = CachedPosition_->Index;
        if (index != (int)leaf->KeySize() - 1) {
            ++index;
            CachedPosition_->Wrapper = leaf->template GetWrapperByIndex<Modifiable>(index);
            return *this;
        }
    }
    const TKey& key = TCowTreeTraits<TKey, TValue>::KeyFromWrapper(CachedPosition_->Wrapper);
    CachedPosition_ = Tree_->FirstSatisfying<Modifiable>([&] (const auto& k) {
        // k > key.
        return Less_(key, k);
    });
    return *this;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool Modifiable>
typename TCowTree<TKey, TValue, BlockSize, TLess>::template TIterator<Modifiable> TCowTree<TKey, TValue, BlockSize, TLess>::TIterator<Modifiable>::operator++(int)
{
    TIterator tmp(*this);
    ++(*this);
    return tmp;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool Modifiable>
typename TCowTree<TKey, TValue, BlockSize, TLess>::template TIterator<Modifiable>& TCowTree<TKey, TValue, BlockSize, TLess>::TIterator<Modifiable>::operator--()
{
    if (!CachedPosition_) {
        CachedPosition_ = Tree_->LastSatisfying<Modifiable>([] (const auto&) {
            return true;
        });
        return *this;
    }

    if (CachedPosition_->IsValid(Tree_->VersionContext_)) {
        auto& leaf = CachedPosition_->Leaf;
        int& index = CachedPosition_->Index;
        if (index != 0) {
            --index;
            CachedPosition_->Wrapper = leaf->template GetWrapperByIndex<Modifiable>(index);
            return *this;
        }
    }

    const TKey& key = TCowTreeTraits<TKey, TValue>::KeyFromWrapper(CachedPosition_->Wrapper);
    CachedPosition_ = Tree_->LastSatisfying<Modifiable>([&] (const auto& k) {
        // k < key.
        return Less_(k, key);
    });
    return *this;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool Modifiable>
typename TCowTree<TKey, TValue, BlockSize, TLess>::template TIterator<Modifiable> TCowTree<TKey, TValue, BlockSize, TLess>::TIterator<Modifiable>::operator--(int)
{
    TIterator tmp(*this);
    --(*this);
    return tmp;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool Modifiable>
TCowTree<TKey, TValue, BlockSize, TLess>::TIterator<Modifiable>::operator TIterator<false>() const&
{
    return TIterator<false>(CachedPosition_, Tree_);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool Modifiable>
TCowTree<TKey, TValue, BlockSize, TLess>::TIterator<Modifiable>::operator TIterator<false>() &&
{
    return TIterator<false>(std::move(CachedPosition_), std::move(Tree_));
}

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue, bool OverrideValue>
std::pair<NDetail::TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>, bool> TCowTree<TKey, TValue, BlockSize, TLess>::Insert(TAnyKeyValue&& keyValue)
{
    Root_ = Root_.AcquireSafe(VersionContext_);
    if (Root_ == nullptr) {
        auto newRoot = NDetail::MaybeInplaceNew<NDetail::TCowTreeLeaf<TKey, TValue, BlockSize, TLess>>(std::forward<TAnyKeyValue>(keyValue));
        NDetail::TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess> result = newRoot->template BuildCachedPosition<false>(0, VersionContext_);
        Root_ = std::move(newRoot);
        return {result, true};
    }

    auto&& result = Root_->template Insert<TAnyKeyValue, OverrideValue>(std::forward<TAnyKeyValue>(keyValue), VersionContext_);
    if (Root_->KeySize() == BlockSize + 1) {
        auto rightNode = Root_->Split();
        auto newRoot = NDetail::MaybeInplaceNew<NDetail::TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>>(std::move(Root_), std::move(rightNode));
        Root_ = std::move(newRoot);
    }
    return result;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTree<TKey, TValue, BlockSize, TLess>::Verify() const
{
    if (Root_) {
        std::pair<int, int> leafDepth = {std::numeric_limits<int>::max(), std::numeric_limits<int>::min()};
        Root_->Verify(leafDepth, 0);
        YT_ASSERT(leafDepth.first == leafDepth.second);
    }
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
size_t TCowTree<TKey, TValue, BlockSize, TLess>::Erase(const TKey& key)
{
    Root_ = Root_.AcquireSafe(VersionContext_);
    if (Root_ == nullptr) {
        return 0;
    }

    auto result = Root_->Erase(key, VersionContext_);
    if (!Root_->IsLeaf_ && Root_->KeySize() == 1) {
        auto rootAsInnerNode = static_cast<NDetail::TCowTreeInnerNode<TKey, TValue, BlockSize, TLess>*>(Root_.Get());
        Root_ = std::move(rootAsInnerNode->Children_[0]);
    }
    return result;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
typename std::optional<NDetail::TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> TCowTree<TKey, TValue, BlockSize, TLess>::FirstSatisfying(const std::function<bool(const TKey&)>& predicate) const
{
    if (!Root_) {
        return {};
    }
    if constexpr (ForModify) {
        auto thisMutable = const_cast<TCowTree*>(this);
        thisMutable->Root_ = thisMutable->Root_.AcquireSafe(thisMutable->VersionContext_);
    }
    using TVersionContextRef = std::conditional_t<ForModify, NDetail::TCowTreeVersionContext&, const NDetail::TCowTreeVersionContext&>;
    return Root_->template FirstSatisfying<ForModify>(predicate, const_cast<TVersionContextRef>(VersionContext_));
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <bool ForModify>
typename std::optional<NDetail::TCowTreeCachedPosition<TKey, TValue, BlockSize, TLess>> TCowTree<TKey, TValue, BlockSize, TLess>::LastSatisfying(const std::function<bool(const TKey&)>& predicate) const
{
    if (!Root_) {
        return {};
    }
    if constexpr (ForModify) {
        auto thisMutable = const_cast<TCowTree*>(this);
        thisMutable->Root_ = thisMutable->Root_.AcquireSafe(thisMutable->VersionContext_);
    }
    using TVersionContextRef = std::conditional_t<ForModify, NDetail::TCowTreeVersionContext&, const NDetail::TCowTreeVersionContext&>;
    return Root_->template LastSatisfying<ForModify>(predicate, const_cast<TVersionContextRef>(VersionContext_));
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
size_t TCowTree<TKey, TValue, BlockSize, TLess>::Size() const
{
    if (!Root_) {
        return 0;
    }
    return Root_->Size();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::const_iterator TCowTree<TKey, TValue, BlockSize, TLess>::cbegin() const
{
    return const_iterator(FirstSatisfying<false>([] (const auto&) {
        return true;
    }),
        this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::const_iterator TCowTree<TKey, TValue, BlockSize, TLess>::begin() const
{
    return cbegin();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator TCowTree<TKey, TValue, BlockSize, TLess>::begin()
{
    return iterator(FirstSatisfying<true>([] (const auto&) {
        return true;
    }),
        this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::const_iterator TCowTree<TKey, TValue, BlockSize, TLess>::cend() const
{
    return const_iterator({}, this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::const_iterator TCowTree<TKey, TValue, BlockSize, TLess>::end() const
{
    return cend();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator TCowTree<TKey, TValue, BlockSize, TLess>::end()
{
    return iterator({}, this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::const_iterator TCowTree<TKey, TValue, BlockSize, TLess>::find(const TKey& key) const
{
    if (!Root_) {
        return cend();
    }
    return const_iterator(Root_->template Find<false>(key, VersionContext_), this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <class TAnyKey>
typename TCowTree<TKey, TValue, BlockSize, TLess>::const_iterator TCowTree<TKey, TValue, BlockSize, TLess>::find(const TAnyKey& key) const
{
    if (!Root_) {
        return cend();
    }
    auto candidate = const_iterator(FirstSatisfying<false>([&] (const auto& k) {
        // k >= key.
        return !Less_(k, key);
    }),
        this);

    if (candidate == cend()) {
        return cend();
    }

    TLess less;
    const auto& currentKey = TCowTreeTraits<TKey, TValue>::KeyFromWrapper(candidate.CachedPosition_->Wrapper);
    if (!less(key, currentKey) && !less(currentKey, key)) {
        return candidate;
    }
    return cend();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator TCowTree<TKey, TValue, BlockSize, TLess>::find(const TKey& key)
{
    if (!Root_) {
        return end();
    }
    Root_ = Root_.AcquireSafe(VersionContext_);
    return iterator(Root_->template Find<true>(key, VersionContext_), this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <class TAnyKey>
typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator TCowTree<TKey, TValue, BlockSize, TLess>::find(const TAnyKey& key)
{
    if (!Root_) {
        return end();
    }
    auto candidate = iterator(FirstSatisfying<true>([&] (const auto& k) {
        // k >= key.
        return !Less_(k, key);
    }),
        this);

    if (candidate == end()) {
        return end();
    }

    TLess less;
    const auto& currentKey = TCowTreeTraits<TKey, TValue>::KeyFromWrapper(candidate.CachedPosition_->Wrapper);
    if (!less(key, currentKey) && !less(currentKey, key)) {
        return candidate;
    }
    return end();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
bool TCowTree<TKey, TValue, BlockSize, TLess>::contains(const TKey& key) const
{
    return find(key) != cend();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <class TAnyKey>
bool TCowTree<TKey, TValue, BlockSize, TLess>::contains(const TAnyKey& key) const
{
    return find(key) != cend();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue>
std::pair<typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator, bool> TCowTree<TKey, TValue, BlockSize, TLess>::insert(TAnyKeyValue&& kv)
{
    auto result = Insert(std::forward<TAnyKeyValue>(kv));
    return {iterator(result.first, this), result.second};
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <std::convertible_to<typename TCowTreeTraits<TKey, TValue>::value_type> TAnyKeyValue>
typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator TCowTree<TKey, TValue, BlockSize, TLess>::insert(const_iterator, TAnyKeyValue&& kv)
{
    return insert(std::forward<TAnyKeyValue>(kv)).first;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTree<TKey, TValue, BlockSize, TLess>::insert(node_type&& node)
{
    insert(std::move(node).value());
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
std::pair<typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator, bool> TCowTree<TKey, TValue, BlockSize, TLess>::insert(const value_type& kv)
{
    auto result = Insert(kv);
    return {iterator(result.first, this), result.second};
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
std::pair<typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator, bool> TCowTree<TKey, TValue, BlockSize, TLess>::insert(value_type&& kv)
{
    auto result = Insert(std::move(kv));
    return {iterator(result.first, this), result.second};
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator TCowTree<TKey, TValue, BlockSize, TLess>::insert(const_iterator, const value_type& kv)
{
    return insert(kv).first;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator TCowTree<TKey, TValue, BlockSize, TLess>::insert(const_iterator, value_type&& kv)
{
    return insert(std::move(kv)).first;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
size_t TCowTree<TKey, TValue, BlockSize, TLess>::erase(const TKey& key)
{
    return Erase(key);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTree<TKey, TValue, BlockSize, TLess>::erase(const_iterator it)
{
    erase(TCowTreeTraits<TKey, TValue>::KeyFromKV(*it));
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
bool TCowTree<TKey, TValue, BlockSize, TLess>::empty() const
{
    return size() == 0;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
size_t TCowTree<TKey, TValue, BlockSize, TLess>::size() const
{
    return Size();
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::node_type TCowTree<TKey, TValue, BlockSize, TLess>::extract(const TKey& key)
{
    auto result = TNodeHandle(*find(key));
    erase(key);
    return result;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::node_type TCowTree<TKey, TValue, BlockSize, TLess>::extract(const_iterator it)
{
    return extract(*it);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
size_t TCowTree<TKey, TValue, BlockSize, TLess>::count(const TKey& key) const
{
    return contains(key) ? 1 : 0;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <class TAnyKey>
size_t TCowTree<TKey, TValue, BlockSize, TLess>::count(const TAnyKey& key) const
{
    return contains(key) ? 1 : 0;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
void TCowTree<TKey, TValue, BlockSize, TLess>::clear()
{
    Root_ = {};
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator TCowTree<TKey, TValue, BlockSize, TLess>::lower_bound(const TKey& key)
{
    return iterator(FirstSatisfying<true>([&] (const auto& k) {
        // k >= key.
        return !Less_(k, key);
    }),
        this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::const_iterator TCowTree<TKey, TValue, BlockSize, TLess>::lower_bound(const TKey& key) const
{
    return const_iterator(FirstSatisfying<false>([&] (const auto& k) {
        // k >= key.
        return !Less_(k, key);
    }),
        this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <class TAnyKey>
typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator TCowTree<TKey, TValue, BlockSize, TLess>::lower_bound(const TAnyKey& key)
{
    return iterator(FirstSatisfying<true>([&] (const auto& k) {
        // k >= key.
        return !Less_(k, key);
    }),
        this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <class TAnyKey>
typename TCowTree<TKey, TValue, BlockSize, TLess>::const_iterator TCowTree<TKey, TValue, BlockSize, TLess>::lower_bound(const TAnyKey& key) const
{
    return const_iterator(FirstSatisfying<false>([&] (const auto& k) {
        // k >= key.
        return !Less_(k, key);
    }),
        this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator TCowTree<TKey, TValue, BlockSize, TLess>::upper_bound(const TKey& key)
{
    return iterator(FirstSatisfying<true>([&] (const auto& k) {
        // k > key.
        return Less_(key, k);
    }),
        this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::const_iterator TCowTree<TKey, TValue, BlockSize, TLess>::upper_bound(const TKey& key) const
{
    return const_iterator(FirstSatisfying<false>([&] (const auto& k) {
        // k > key.
        return Less_(key, k);
    }),
        this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <class TAnyKey>
typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator TCowTree<TKey, TValue, BlockSize, TLess>::upper_bound(const TAnyKey& key)
{
    return iterator(FirstSatisfying<true>([&] (const auto& k) {
        // k > key.
        return Less_(key, k);
    }),
        this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
template <class TAnyKey>
typename TCowTree<TKey, TValue, BlockSize, TLess>::const_iterator TCowTree<TKey, TValue, BlockSize, TLess>::upper_bound(const TAnyKey& key) const
{
    return const_iterator(FirstSatisfying<false>([&] (const auto& k) {
        // k > key.
        return Less_(key, k);
    }),
        this);
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
std::add_lvalue_reference_t<TValue> TCowTree<TKey, TValue, BlockSize, TLess>::at(const TKey& key)
    requires(!std::is_void_v<TValue>)
{
    // When we pass insert, we already copy-on-write all the required nodes even if no actual insert had been performed.
    // Thus it is safe to modify the value under this iterator.
    auto wrapper = Insert<std::pair<const TKey, TValue>&&, true>(std::pair<const TKey, TValue>{key, TValue()}).first;
    return TCowTreeTraits<TKey, TValue>::KVFromWrapper(wrapper.Wrapper).second;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
std::add_lvalue_reference_t<const TValue> TCowTree<TKey, TValue, BlockSize, TLess>::at(const TKey& key) const
    requires(!std::is_void_v<TValue>)
{
    auto it = find(key);
    YT_ASSERT(it != cend());
    return it->second;
}

template <class TKey, class TValue, size_t BlockSize, class TLess>
typename TCowTree<TKey, TValue, BlockSize, TLess>::iterator TCowTree<TKey, TValue, BlockSize, TLess>::FindFirstSatisfying(const std::function<bool(const TKey&)>& predicate)
{
    return iterator(FirstSatisfying<true>(predicate), this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
