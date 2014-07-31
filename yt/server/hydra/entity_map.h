#pragma once

#include "public.h"
#include "composite_automaton.h"

#include <core/concurrency/thread_affinity.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

template <
    class TKey,
    class TValue,
    class THash = ::THash<TKey>
>
struct IReadOnlyEntityMap
{
    typedef yhash_map<TKey, TValue*, THash> TMap;
    typedef std::pair<TKey, TValue*> TItem;
    typedef typename TMap::iterator TIterator;
    typedef typename TMap::const_iterator TConstIterator;

    // STL interop.
    typedef TKey key_type;
    typedef std::pair<const TKey, TValue*> value_type;
    typedef TValue* mapped_type;

    virtual ~IReadOnlyEntityMap()
    { }

    virtual TValue* Find(const TKey& key) const = 0;
    virtual TValue* Get(const TKey& key) const = 0;

    virtual bool Contains(const TKey& key) const = 0;

    virtual int GetSize() const = 0;

    virtual TConstIterator Begin() const = 0;
    virtual TConstIterator End() const = 0;

    // STL interop.
    TConstIterator begin() const;
    TConstIterator end() const;
    size_t size() const;

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
    explicit TEntityMap(const TTraits& traits = TTraits());
    virtual ~TEntityMap();

    // IReadOnlyEntityMap implementation (not-mutating methods).
    // NB: Declaring these methods virtual should not cause any overhead due
    // to GCC devirtualization capabilities.

    virtual TValue* Find(const TKey& key) const override;

    virtual TValue* Get(const TKey& key) const override;

    virtual bool Contains(const TKey& key) const override;

    virtual int GetSize() const override;

    // Other (possibly mutating) methods.

    void Insert(const TKey& key, TValue* value);

    bool TryRemove(const TKey& key);
    void Remove(const TKey& key);

    std::unique_ptr<TValue> Release(const TKey& key);

    void Clear();

    virtual TConstIterator Begin() const override;
    virtual TConstIterator End() const override;

    using IReadOnlyEntityMap::Begin;
    TIterator Begin();

    using IReadOnlyEntityMap::End;
    TIterator End();

    void SaveKeys(TSaveContext& context) const;
        
    template <class TContext>
    void SaveValues(TContext& context) const;

    void LoadKeys(TLoadContext& context);

    template <class TContext>
    void LoadValues(TContext& context);

    // STL interop.
    using IReadOnlyEntityMap::begin;
    TIterator begin();

    using IReadOnlyEntityMap::end;
    TIterator end();

private:
    DECLARE_THREAD_AFFINITY_SLOT(UserThread);

    TMap Map;
    TTraits Traits;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

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
