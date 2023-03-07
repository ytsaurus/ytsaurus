#pragma once

#include "public.h"

#include <util/system/spinlock.h>

#include <util/generic/hash_set.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TInternRegistry
    : public TIntrinsicRefCounted
{
public:
    TInternedObject<T> Intern(T&& data);
    TInternedObject<T> Intern(const T& data);
    int GetSize() const;
    void Clear();

private:
    friend class TInternedObjectData<T>;

    void OnInternedDataDestroyed(TInternedObjectData<T>* data);

    template <class F>
    TInternedObject<T> DoIntern(const T& data, const F& internedDataBuilder);

    struct THash
    {
        size_t operator() (const TInternedObjectData<T>* internedData) const;
        size_t operator() (const T& data) const;
    };

    struct TEqual
    {
        bool operator() (const TInternedObjectData<T>* lhs, const TInternedObjectData<T>* rhs) const;
        bool operator() (const TInternedObjectData<T>* lhs, const T& rhs) const;
    };

    TSpinLock Lock_;

    using TRegistrySet = THashSet<TInternedObjectData<T>*, THash, TEqual>;
    TRegistrySet Registry_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TInternedObjectData
    : public TIntrinsicRefCounted
{
public:
    ~TInternedObjectData();

    static TInternedObjectDataPtr<T> GetDefault();

    const T& GetData() const;
    size_t GetHash() const;

private:
    friend class TInternRegistry<T>;
    DECLARE_NEW_FRIEND();

    const T Data_;
    const size_t Hash_;
    const TInternRegistryPtr<T> Registry_;
    typename TInternRegistry<T>::TRegistrySet::iterator Iterator_;

    TInternedObjectData(const T& data, TInternRegistryPtr<T> registry);
    TInternedObjectData(T&& data, TInternRegistryPtr<T> registry);
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TInternedObject
{
public:
    TInternedObject();

    TInternedObject(const TInternedObject<T>& other) = default;
    TInternedObject(TInternedObject<T>&& other) = default;

    TInternedObject<T>& operator=(const TInternedObject<T>& other) = default;
    TInternedObject<T>& operator=(TInternedObject<T>&& other) = default;

    const T& operator*() const;
    const T* operator->() const;

    TInternedObjectDataPtr<T> ToData() const;
    static TInternedObject<T> FromData(TInternedObjectDataPtr<T> data);

    static bool RefEqual(const TInternedObject<T>& lhs, const TInternedObject<T>& rhs);

private:
    friend class TInternRegistry<T>;

    TInternedObjectDataPtr<T> Data_;

    explicit TInternedObject(TInternedObjectDataPtr<T> data);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define INTERN_REGISTRY_INL_H_
#include "intern_registry-inl.h"
#undef INTERN_REGISTRY_INL_H_
