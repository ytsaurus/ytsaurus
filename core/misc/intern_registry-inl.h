#pragma once
#ifndef INTERN_REGISTRY_INL_H_
#error "Direct inclusion of this file is not allowed, include intern_registry.h"
// For the sake of sane code completion.
#include "intern_registry.h"
#endif

#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TInternedObject<T> TInternRegistry<T>::Intern(T&& data)
{
    return DoIntern(data, [&] {
        return  New<TInternedObjectData<T>>(std::move(data), this);
    });
}

template <class T>
TInternedObject<T> TInternRegistry<T>::Intern(const T& data)
{
    return DoIntern(data, [&] {
        return  New<TInternedObjectData<T>>(data, this);
    });
}

template <class T>
template <class F>
TInternedObject<T> TInternRegistry<T>::DoIntern(const T& data, const F& internedDataBuilder)
{
    if (TInternedObjectData<T>::GetDefault()->Data_ == data) {
        return TInternedObject<T>();
    }
    auto guard = Guard(Lock_);
    auto it = Registry_.find(data);
    if (it == Registry_.end()) {
        auto internedData = internedDataBuilder();
        it = Registry_.insert(internedData.Get()).first;
        internedData->Iterator_ = it;
        return TInternedObject<T>(std::move(internedData));
    } else {
        return TInternedObject<T>(MakeStrong(*it));
    }
}

template <class T>
int TInternRegistry<T>::GetSize() const
{
    auto guard = Guard(Lock_);
    return static_cast<int>(Registry_.size());
}

template <class T>
void TInternRegistry<T>::OnInternedDataDestroyed(TInternedObjectData<T>* data)
{
    auto guard = Guard(Lock_);
    Registry_.erase(data->Iterator_);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
size_t TInternRegistry<T>::THash::operator()(const TInternedObjectData<T>* internedData) const
{
    return internedData->GetHash();
}

template <class T>
size_t TInternRegistry<T>::THash::operator()(const T& data) const
{
    return ::THash<T>()(data);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool TInternRegistry<T>::TEqual::operator()(
    const TInternedObjectData<T>* lhs,
    const TInternedObjectData<T>* rhs) const
{
    return lhs == rhs;
}

template <class T>
bool TInternRegistry<T>::TEqual::operator()(
    const TInternedObjectData<T>* lhs,
    const T& rhs) const
{
    return lhs->GetData() == rhs;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TInternedObjectData<T>::~TInternedObjectData()
{
    if (Registry_) {
        Registry_->OnInternedDataDestroyed(this);
    }
}

template <class T>
TInternedObjectDataPtr<T> TInternedObjectData<T>::GetDefault()
{
    static const auto Default = New<TInternedObjectData>(T(), nullptr);
    return Default;
}

template <class T>
const T& TInternedObjectData<T>::GetData() const
{
    return Data_;
}

template <class T>
size_t TInternedObjectData<T>::GetHash() const
{
    return Hash_;
}

template <class T>
TInternedObjectData<T>::TInternedObjectData(T&& data, TInternRegistryPtr<T> registry)
    : Data_(std::move(data))
    , Hash_(THash<T>()(Data_))
    , Registry_(std::move(registry))
{ }

template <class T>
TInternedObjectData<T>::TInternedObjectData(const T& data, TInternRegistryPtr<T> registry)
    : Data_(data)
    , Hash_(THash<T>()(Data_))
    , Registry_(std::move(registry))
{ }

////////////////////////////////////////////////////////////////////////////////

template <class T>
TInternedObject<T>::TInternedObject()
    : Data_(TInternedObjectData<T>::GetDefault())
{ }

template <class T>
const T& TInternedObject<T>::operator*() const
{
    return Data_->GetData();
}

template <class T>
const T* TInternedObject<T>::operator->() const
{
    return &Data_->GetData();
}

template <class T>
void* TInternedObject<T>::ToRaw() const
{
    return Data_.Get();
}

template <class T>
TInternedObject<T> TInternedObject<T>::FromRaw(void* raw)
{
    return TInternedObject<T>(static_cast<TInternedObjectData<T>*>(raw));
}

template<class T>
bool TInternedObject<T>::RefEqual(const TInternedObject <T>& lhs, const TInternedObject <T>& rhs)
{
    return lhs.Data_ == rhs.Data_;
}

template <class T>
TInternedObject<T>::TInternedObject(TInternedObjectDataPtr<T> data)
    : Data_(std::move(data))
{ }

////////////////////////////////////////////////////////////////////////////////

struct TInternedObjectSerializer
{
    static inline const TEntitySerializationKey InlineKey = TEntitySerializationKey(-3);

    template <class C, class T>
    static void Save(C& context, const TInternedObject<T>& object)
    {
        using NYT::Save;

        auto it = context.SavedInternedObjects().find(object.ToRaw());
        if (it == context.SavedInternedObjects().end()) {
            Save(context, InlineKey);
            Save(context, *object);
            YT_VERIFY(context.SavedInternedObjects().emplace(object.ToRaw(), context.GenerateSerializationKey()).second);
        } else {
            Save(context, it->second);
        }
    }

    template <class C, class T>
    static void Load(C& context, TInternedObject<T>& object)
    {
        using NYT::Load;

        auto key = NYT::LoadSuspended<TEntitySerializationKey>(context);
        if (key == InlineKey) {
            SERIALIZATION_DUMP_INDENT(context) {
                auto value = Load<T>(context);
                const auto& registry = context.template GetInternRegistry<T>();
                object = registry->Intern(std::move(value));
                auto key = context.RegisterEntity(object.ToRaw());
                SERIALIZATION_DUMP_WRITE(context, "objref %v", key.Index);
            }
        } else {
            object = TInternedObject<T>::FromRaw(context.template GetEntity<void*>(key));
            SERIALIZATION_DUMP_WRITE(context, "objref %v", key.Index);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class T, class C>
struct TSerializerTraits<
    TInternedObject<T>,
    C,
    void
>
{
    using TSerializer = TInternedObjectSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
