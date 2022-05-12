#pragma once

#include "public.h"

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Fiber-local key-value storage that is able to propagate between fibers.
//!
//! When a new callback is created via BIND, then a copy is made into its bind state. So,
//! this storage is propagated between the fibers creating each other.
//!
//! It is a key value storage where keys are type names. Thus, you may consider it as a
//! set of singletons.
//!
//! TPropagatingStorage is copy-on-write, so copying it between fibers is cheap if no values
//! are modified.
class TPropagatingStorage
{
public:
    //! Creates a null storage.
    //! When the storage is null, you cannot read from it or write into it.
    TPropagatingStorage();

    //! Creates an empty, non-null storage.
    static TPropagatingStorage Create();

    ~TPropagatingStorage();

    TPropagatingStorage(const TPropagatingStorage& other);
    TPropagatingStorage(TPropagatingStorage&& other);

    TPropagatingStorage& operator=(const TPropagatingStorage& other);
    TPropagatingStorage& operator=(TPropagatingStorage&& other);

    //! Returns true if the storage is null.
    //! When the storage is null, you cannot read from it or write into it.
    bool IsNull() const;

    template <class T>
    bool Has() const;

    template <class T>
    const T& GetOrCrash() const;

    template <class T>
    const T* TryGet() const;

    template <class T>
    std::optional<T> Exchange(T value);

    template <class T>
    std::optional<T> Remove();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

    explicit TPropagatingStorage(TIntrusivePtr<TImpl> impl);

    const std::any* GetRaw(const std::type_info& typeInfo) const;
    std::optional<std::any> ExchangeRaw(std::any value);
    std::optional<std::any> RemoveRaw(const std::type_info& typeInfo);

    void EnsureUnique();
};

////////////////////////////////////////////////////////////////////////////////

TPropagatingStorage& GetCurrentPropagatingStorage();
TPropagatingStorage& GetOrCreateCurrentPropagatingStorage();
TPropagatingStorage SwapCurrentPropagatingStorage(TPropagatingStorage storage);

////////////////////////////////////////////////////////////////////////////////

class TPropagatingStorageGuard
{
public:
    explicit TPropagatingStorageGuard(TPropagatingStorage storage);
    ~TPropagatingStorageGuard();

    TPropagatingStorageGuard(const TPropagatingStorageGuard& other) = delete;
    TPropagatingStorageGuard(TPropagatingStorageGuard&& other) = delete;
    TPropagatingStorageGuard& operator=(const TPropagatingStorageGuard& other) = delete;
    TPropagatingStorageGuard& operator=(TPropagatingStorageGuard&& other) = delete;

private:
    TPropagatingStorage OldStorage_;
};

template <class T>
class TPropagatingValueGuard
{
public:
    explicit TPropagatingValueGuard(T value);
    ~TPropagatingValueGuard();

    TPropagatingValueGuard(const TPropagatingValueGuard& other) = delete;
    TPropagatingValueGuard(TPropagatingValueGuard&& other) = delete;
    TPropagatingValueGuard& operator=(const TPropagatingValueGuard& other) = delete;
    TPropagatingValueGuard& operator=(TPropagatingValueGuard&& other) = delete;

private:
    std::optional<T> OldValue_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define PROPAGATING_STORAGE_INL_H_
#include "propagating_storage-inl.h"
#undef PROPAGATING_STORAGE_INL_H_
