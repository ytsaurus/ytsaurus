#pragma once

#include <library/cpp/yt/assert/assert.h>

#include <util/system/types.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <type_traits>
#include <utility>

#ifndef COW_TREE_VALUE_ENABLE_DEBUG
    #ifdef NDEBUG
        #define COW_TREE_VALUE_ENABLE_DEBUG 0
    #else
        #define COW_TREE_VALUE_ENABLE_DEBUG 1
    #endif
#endif

namespace NYT::NFlow::NCowTreeInternals {

////////////////////////////////////////////////////////////////////////////////

//! In-place storage for a slot whose lifetime is managed by its node.
template <class T>
class TValueHolder
{
public:
    TValueHolder();
    ~TValueHolder();

    TValueHolder(const TValueHolder&) = delete;
    TValueHolder& operator=(const TValueHolder&) = delete;

    template <class... U>
    void Set(U&&... u) noexcept;

    void Replace(T&& replacement) noexcept;
    void CopyFrom(const TValueHolder<T>& holder) noexcept;
    void MoveFrom(TValueHolder<T>&& holder);

    T& Get() &;
    T&& Get() &&;
    const T& Get() const&;
    const T&& Get() const&&;

    void Clear();

private:
    union
    {
        char Dummy[sizeof(T)] = {};
        T Data;
    };
#if COW_TREE_VALUE_ENABLE_DEBUG
    bool DebugIsEmpty_ = true;
#endif

    void DebugSetEmpty();
    void DebugSetNotEmpty();
    void DebugCheckEmpty() const;
    void DebugCheckNotEmpty() const;
};

////////////////////////////////////////////////////////////////////////////////

//! Reference counter for tree nodes in single-threaded configurations.
//! A node is born with one reference, owned by its creator.
class TPlainRefCounter
{
public:
    void Ref() noexcept;
    //! Returns true when the last reference is gone.
    bool Unref() noexcept;

private:
    i64 Count_ = 1;
};

//! Thread-safe counterpart: snapshots may release nodes from other threads.
class TAtomicRefCounter
{
public:
    void Ref() noexcept;
    //! Returns true when the last reference is gone.
    bool Unref() noexcept;

private:
    std::atomic<i64> Count_{1};
};

//! No-op mutex for single-threaded configurations.
struct TNullMutex
{
    void Acquire() noexcept;

    void Release() noexcept;
};

////////////////////////////////////////////////////////////////////////////////

//! Intrusive smart pointer over refcounted tree nodes; |TOps| supplies
//! Ref/Unref (the latter destroys the node when the last reference is gone).
//!
//! Ownership convention: reference counts measure *sharing*, not parent-child
//! edges. A raw child slot inside an inner node carries exactly one reference
//! unit, so structural moves within one tree never touch the counters; they are
//! only touched when a node gets shared (CoW, snapshots) or released.
template <class T, class TOps>
class TRef
{
public:
    TRef() = default;

    //! Adopts an existing reference unit.
    explicit TRef(T* node) noexcept;

    TRef(const TRef& other) noexcept;
    TRef(TRef&& other) noexcept;
    TRef& operator=(TRef other) noexcept;
    ~TRef();

    T* Get() const noexcept;
    T* operator->() const noexcept;
    explicit operator bool() const noexcept;

    //! Detaches the raw pointer together with its reference unit.
    T* Release() noexcept;

    void Reset() noexcept;

private:
    T* Node_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
void HolderOrValueAssign(T& dst, const T& src);

template <class T>
void HolderOrValueAssign(T& dst, T&& src);

template <class T>
void HolderOrValueAssign(TValueHolder<T>& dst, const TValueHolder<T>& src);

template <class T>
void HolderOrValueAssign(TValueHolder<T>& dst, TValueHolder<T>&& src);

template <class T>
void HolderOrValueAssign(TValueHolder<T>& dst, const T& src);

template <class T>
void HolderOrValueAssign(TValueHolder<T>& dst, T&& src);

template <class T>
void HolderOrValueClear(T&);

template <class T>
void HolderOrValueClear(TValueHolder<T>& dst);

//! Whether element-wise move/copy of an array of T is equivalent to one bulk
//! memmove/memcpy. Holders of trivially copyable payloads qualify only in release
//! builds: a debug holder carries an occupancy flag with move-out semantics.
template <class T>
constexpr bool HolderOrValueBulkCopyable = std::is_trivially_copyable_v<T>;

template <class T>
constexpr bool HolderOrValueBulkCopyable<TValueHolder<T>> =
#if COW_TREE_VALUE_ENABLE_DEBUG
    false;
#else
    std::is_trivially_copyable_v<T>;
#endif

template <class T>
void HolderOrValueMove(T* from, T* to, T* dst);

template <class T>
void HolderOrValueMoveBackward(T* from, T* to, T* dst);

template <class T>
void HolderCopyRange(const TValueHolder<T>* from, const TValueHolder<T>* to, TValueHolder<T>* dst);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCowTreeInternals

#define COW_TREE_VALUE_INL_H
#include "cow_tree_value-inl.h"
#undef COW_TREE_VALUE_INL_H
