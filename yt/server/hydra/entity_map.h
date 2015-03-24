#pragma once

#include "public.h"
#include "composite_automaton.h"

#include <core/concurrency/thread_affinity.h>

#include <core/misc/chunked_memory_pool.h>

#include <type_traits>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
struct TDefaultEntityMapTraits
{
    std::unique_ptr<TValue> Create(const TKey& key) const;
};

////////////////////////////////////////////////////////////////////////////////

//! A common base for all structures representing a highly mutable data
//! associated with entities within Hydra.
struct TEntityDynamicDataBase
{
    TEntitySerializationKey SerializationKey;
};

////////////////////////////////////////////////////////////////////////////////

//! A common base for all entities within Hydra.
class TEntityBase
    : private TNonCopyable
{
public:
    //! Returns the pointer to the highly mutable data associated with the entity.
    /*
     *  Inheritors may hide this one with another |GetDynamicData| method
     *  returning the actual type derived from TEntityDynamicDataBase.
     */
    TEntityDynamicDataBase* GetDynamicData() const;

    //! Sets the pointer to the highly mutable data associated with the entity.
    void SetDynamicData(TEntityDynamicDataBase* data);

protected:
    //! A helper for implementing |GetDynamicData| in inheritors.
    template <class T>
    T* GetTypedDynamicData() const;

private:
    TEntityDynamicDataBase* DynamicData_ = nullptr;

};

////////////////////////////////////////////////////////////////////////////////

//! Overlayed with the spare entities of regular dynamic data.
struct TSpareEntityDynamicData
{
    TSpareEntityDynamicData* Next;
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
    using TDynamicData = typename std::decay<decltype(*static_cast<TValue*>(nullptr)->GetDynamicData())>::type;

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

    TChunkedMemoryPool DynamicDataPool_;
    TSpareEntityDynamicData* FirstSpareDynamicData_ = nullptr;

    std::vector<TKey> LoadKeys_;
    std::vector<TValue*> LoadValues_;
    mutable std::vector<typename TMap::const_iterator> SaveIterators_;


    TDynamicData* AllocateDynamicData();
    void FreeDynamicData(TDynamicData* data);

    void DoClear();

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
