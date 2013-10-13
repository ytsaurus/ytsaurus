#pragma once

#include "public.h"
#include "composite_automaton.h"

#include <core/concurrency/thread_affinity.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

template <
    class TKey,
    class TValue
>
struct IReadOnlyEntityMap
{
    virtual ~IReadOnlyEntityMap()
    { }

    virtual TValue* Find(const TKey& key) const = 0;
    virtual TValue* Get(const TKey& key) const = 0;

    virtual bool Contains(const TKey& key) const = 0;

    virtual int GetSize() const = 0;

    virtual std::vector<TKey> GetKeys(size_t sizeLimit = std::numeric_limits<size_t>::max()) const = 0;
    virtual std::vector<TValue*> GetValues(size_t sizeLimit = std::numeric_limits<size_t>::max()) const = 0;

};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TValue>
struct TDefaultEntityMapTraits
{
    std::unique_ptr<TValue> Create(const TKey& key) const;
};

////////////////////////////////////////////////////////////////////////////////

template <
    class TKey,
    class TValue,
    class TTraits = TDefaultEntityMapTraits<TKey, TValue>,
    class THash = ::THash<TKey>
>
class TEntityMap
    : public IReadOnlyEntityMap<TKey, TValue>
{
public:
    typedef TEntityMap<TKey, TValue, TTraits, THash> TThis;
    typedef yhash_map<TKey, TValue*, THash> TMap;
    typedef typename TMap::iterator TIterator;
    typedef typename TMap::iterator TConstIterator;
    typedef std::pair<TKey, TValue*> TItem;

    explicit TEntityMap(const TTraits& traits = TTraits());
    virtual ~TEntityMap();

    // IReadOnlyEntityMap implementation (not-mutating methods).
    // NB: Declaring these methods virtual should not cause any overhead due
    // to GCC devirtualization capabilities.

    virtual TValue* Find(const TKey& key) const override;

    virtual TValue* Get(const TKey& key) const override;

    virtual bool Contains(const TKey& key) const override;

    virtual int GetSize() const override;

    virtual std::vector<TKey> GetKeys(size_t sizeLimit = Max<size_t>()) const override;

    virtual std::vector<TValue*> GetValues(size_t sizeLimit = Max<size_t>()) const override;

    // Other (possibly mutating) methods.

    void Insert(const TKey& key, TValue* value);

    bool TryRemove(const TKey& key);
    void Remove(const TKey& key);

    std::unique_ptr<TValue> Release(const TKey& key);

    void Clear();

    TIterator Begin();
    TIterator End();

    TConstIterator Begin() const;
    TConstIterator End() const;

    void SaveKeys(TSaveContext& context) const;
        
    template <class TContext>
    void SaveValues(TContext& context) const;

    void LoadKeys(TLoadContext& context);

    template <class TContext>
    void LoadValues(TContext& context);

private:
    DECLARE_THREAD_AFFINITY_SLOT(UserThread);

    TMap Map;
    TTraits Traits;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

// Foreach interop.

template <class TKey, class TValue, class THash>
inline auto begin(NYT::NHydra::TEntityMap<TKey, TValue, THash>& collection) -> decltype(collection.Begin())
{
    return collection.Begin();
}

template <class TKey, class TValue, class THash>
inline auto end(NYT::NHydra::TEntityMap<TKey, TValue, THash>& collection) -> decltype(collection.End())
{
    return collection.End();
}

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_ENTITY_MAP_ACCESSORS(entityName, entityType, idType) \
    entityType* Find ## entityName(const idType& id); \
    entityType* Get ## entityName(const idType& id); \
    const ::NYT::NHydra::IReadOnlyEntityMap<idType, entityType>& entityName ## s() const;

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
    const ::NYT::NHydra::IReadOnlyEntityMap<idType, entityType>& declaringType::entityName ## s() const \
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
    const ::NYT::NHydra::IReadOnlyEntityMap<idType, entityType>& declaringType::entityName ## s() const \
    { \
        return (delegateTo).entityName ## s(); \
    }

////////////////////////////////////////////////////////////////////////////////

#define ENTITY_MAP_INL_H_
#include "entity_map-inl.h"
#undef ENTITY_MAP_INL_H_
