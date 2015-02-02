#pragma once

#include "public.h"
#include "composite_automaton.h"

#include <core/concurrency/thread_affinity.h>

#include <core/misc/chunked_memory_pool.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
struct TDefaultEntityMapTraits
{
    std::unique_ptr<TValue> Create(const TKey& key) const;
};

////////////////////////////////////////////////////////////////////////////////

//! A typical common base for all entities within a Hydra replicated state.
class TEntityBase
    : private TNonCopyable
{
public:
    //! Return the pointer to key attached to the object during serialization.
    TEntitySerializationKey* GetSerializationKeyPtr() const;

    //! Sets the pointer to key attached to the object during serialization.
    void SetSerializationKeyPtr(TEntitySerializationKey* ptr);

private:
    TEntitySerializationKey* SerializationKeyPtr_ = nullptr;

};

////////////////////////////////////////////////////////////////////////////////

template <
    class TKey,
    class TValue,
    class THash = ::THash<TKey>
>
class TReadOnlyEntityMap
{
protected:
    typedef yhash_map<TKey, TValue*, THash> TMap;

public:
    class TIterator
    {
    public:
        std::pair<const TKey, TValue*> operator*() const;

        TIterator& operator++();
        TIterator& operator--();
        TIterator& operator++(int);
        TIterator& operator--(int);

        bool operator==(const TIterator& other) const;
        bool operator!=(const TIterator& other) const;

    private:
        friend class TReadOnlyEntityMap;

        explicit TIterator(typename TMap::const_iterator iterator);

        typename TMap::const_iterator Iterator_;

    };

    TValue* Find(const TKey& key) const;
    TValue* Get(const TKey& key) const;

    bool Contains(const TKey& key) const;

    TIterator Begin() const;
    TIterator End() const;
    int GetSize() const;

    // STL interop.
    typedef TKey key_type;
    typedef TValue* mapped_type;
    typedef std::pair<const TKey, TValue*> value_type;

    TIterator begin() const;
    TIterator end() const;
    size_t size() const;

protected:
    DECLARE_THREAD_AFFINITY_SLOT(UserThread);

    TMap Map_;

};

////////////////////////////////////////////////////////////////////////////////

template <
    class TKey,
    class TValue,
    class TTraits = TDefaultEntityMapTraits<TKey, TValue>,
    class THash = ::THash<TKey>
>
class TEntityMap
    : public TReadOnlyEntityMap<TKey, TValue, THash>
{
public:
    explicit TEntityMap(const TTraits& traits = TTraits());
    ~TEntityMap();

    void Insert(const TKey& key, TValue* value);

    bool TryRemove(const TKey& key);
    void Remove(const TKey& key);

    std::unique_ptr<TValue> Release(const TKey& key);

    void Clear();

    template <class TContext>
    void SaveKeys(TContext& context) const;

    template <class TContext>
    void SaveValues(TContext& context) const;

    template <class TContext>
    void LoadKeys(TContext& context);

    template <class TContext>
    void LoadValues(TContext& context);

private:
    typedef typename TReadOnlyEntityMap<TKey, TValue, THash>::TMap TMap;

    TTraits Traits_;

    TChunkedMemoryPool SerializationKeysPool_;
    std::vector<TEntitySerializationKey*> SpareSerializationKeyPtrs_;

    std::vector<TKey> LoadKeys_;
    std::vector<TValue*> LoadValues_;
    mutable std::vector<typename TMap::const_iterator> SaveIterators_;


    TEntitySerializationKey* AllocateSerializationKeyPtr();
    void FreeSerializationKeyPtr(TEntitySerializationKey* ptr);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

#define DECLARE_ENTITY_MAP_ACCESSORS(entityName, entityType, idType) \
    entityType* Find ## entityName(const idType& id); \
    entityType* Get ## entityName(const idType& id); \
    const ::NYT::NHydra::TReadOnlyEntityMap<idType, entityType>& entityName ## s() const;

#define DEFINE_ENTITY_MAP_ACCESSORS(declaringType, entityName, entityType, idType, map) \
    entityType* declaringType::Find ## entityName(const idType& id) \
    { \
        return (map).Find(id); \
    } \
    \
    entityType* declaringType::Get ## entityName(const idType& id) \
    { \
        return (map).Get(id); \
    } \
    \
    const ::NYT::NHydra::TReadOnlyEntityMap<idType, entityType>& declaringType::entityName ## s() const \
    { \
        return (map); \
    }

#define DELEGATE_ENTITY_MAP_ACCESSORS(declaringType, entityName, entityType, idType, delegateTo) \
    entityType* declaringType::Find ## entityName(const idType& id) \
    { \
        return (delegateTo).Find ## entityName(id); \
    } \
    \
    entityType* declaringType::Get ## entityName(const idType& id) \
    { \
        return (delegateTo).Get ## entityName(id); \
    } \
    \
    const ::NYT::NHydra::TReadOnlyEntityMap<idType, entityType>& declaringType::entityName ## s() const \
    { \
        return (delegateTo).entityName ## s(); \
    }

////////////////////////////////////////////////////////////////////////////////

#define ENTITY_MAP_INL_H_
#include "entity_map-inl.h"
#undef ENTITY_MAP_INL_H_
