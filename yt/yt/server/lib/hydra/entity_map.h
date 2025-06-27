#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <type_traits>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
struct TDefaultEntityMapTraits
{
    int GetParallelSaveBatchSize() const;
    std::unique_ptr<TValue> Create(const TEntityKey<TValue>& key) const;
};

////////////////////////////////////////////////////////////////////////////////

//! A common base for all structures representing a highly mutable data
//! associated with entities within Hydra.
struct TEntityDynamicDataBase
{
    TEntitySerializationKey SerializationKey = NullEntitySerializationKey;
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

template <class TValue>
class TReadOnlyEntityMap
{
protected:
    using TKey = TEntityKey<TValue>;
    using THash = TEntityHash<TValue>;
    using TMapType = THashMap<TKey, TValue*, THash>;

public:
    class TIterator
    {
    public:
        using value_type = TMapType::const_iterator::value_type;
        using difference_type = TMapType::const_iterator::difference_type;

        TIterator();

        const std::pair<const TKey, TValue*>& operator*() const;
        const std::pair<const TKey, TValue*>* operator->() const;

        TIterator& operator++();
        TIterator& operator--();
        TIterator operator++(int);
        TIterator operator--(int);

        bool operator==(const TIterator& other) const;

    private:
        friend class TReadOnlyEntityMap;

        explicit TIterator(typename TMapType::const_iterator iterator);

        typename TMapType::const_iterator Iterator_;
    };

    TValue* Find(const TKey& key) const;
    TValue* Get(const TKey& key) const;

    bool Contains(const TKey& key) const;

    TIterator Begin() const;
    TIterator End() const;
    i64 GetSize() const;

    // STL interop.
    using key_type = TKey;
    using mapped_type = TValue*;
    using value_type = std::pair<const TKey, TValue*>;

    TIterator begin() const;
    TIterator end() const;
    size_t size() const;
    bool empty() const;

protected:
    TMapType Map_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
class TMutableEntityMap
    : public TReadOnlyEntityMap<TValue>
{
public:
    using typename TReadOnlyEntityMap<TValue>::TKey;
    using TDynamicData = typename std::decay<decltype(*static_cast<TValue*>(nullptr)->GetDynamicData())>::type;

    TMutableEntityMap();
    ~TMutableEntityMap();

    TValue* Insert(const TKey& key, std::unique_ptr<TValue> valueHolder);

    bool TryRemove(const TKey& key);
    void Remove(const TKey& key);

    std::unique_ptr<TValue> Release(const TKey& key);

    void Clear();

protected:
    using typename TReadOnlyEntityMap<TValue>::TMapType;

    DECLARE_THREAD_AFFINITY_SLOT(UserThread);

    TChunkedMemoryPool DynamicDataPool_;
    TSpareEntityDynamicData* FirstSpareDynamicData_ = nullptr;

    TDynamicData* AllocateDynamicData();
    void FreeDynamicData(TDynamicData* data);

    void DoClear();

    static std::string GetTypeName();
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue, class TTraits>
class TEntityMap
    : public TMutableEntityMap<TValue>
{
public:
    using typename TMutableEntityMap<TValue>::TKey;

    explicit TEntityMap(const TTraits& traits = TTraits());

    template <class TContext>
    void SaveKeys(TContext& context) const;

    template <class TContext>
    void SaveValues(TContext& context) const;

    template <class TContext>
    void SaveValuesParallel(TContext& context) const;

    template <class TContext>
    void LoadKeys(TContext& context);

    template <class TContext>
    void LoadValues(TContext& context, std::optional<int> firstBatchEntityCount = {});

    template <class TContext>
    void LoadValuesParallel(TContext& context);

private:
    using typename TMutableEntityMap<TValue>::TMapType;

    const TTraits Traits_;

    // COMPAT(danilalexeev): YT-24017.
    static constexpr int AllEntitiesBatchEntityCount = -1;
    static constexpr size_t BatchedFormatMarker = std::numeric_limits<ui32>::max();

    std::vector<TKey> LoadKeys_;
    std::vector<TValue*> LoadValues_;
    mutable std::vector<typename TMapType::const_iterator> SaveIterators_;
};

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS_IMPL(entityName, entityNamePlural, entityType) \
    virtual entityType* Find ## entityName(const ::NYT::NHydra::TEntityKey<entityType>& id) const = 0; \
    virtual entityType* Get ## entityName(const ::NYT::NHydra::TEntityKey<entityType>& id) const = 0; \
    virtual const ::NYT::NHydra::TReadOnlyEntityMap<entityType>& entityNamePlural() const = 0

#define DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(entityName, entityType) \
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS_IMPL(entityName, entityName ## s, entityType)

#define DECLARE_INTERFACE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(entityName, entityNamePlural, entityType) \
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS_IMPL(entityName, entityNamePlural, entityType)

#define DECLARE_ENTITY_MAP_ACCESSORS_IMPL(entityName, entityNamePlural, entityType) \
    entityType* Find ## entityName(const ::NYT::NHydra::TEntityKey<entityType>& id) const; \
    entityType* Get ## entityName(const ::NYT::NHydra::TEntityKey<entityType>& id) const; \
    const ::NYT::NHydra::TReadOnlyEntityMap<entityType>& entityNamePlural() const

#define DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE_IMPL(entityName, entityNamePlural, entityType) \
    entityType* Find ## entityName(const ::NYT::NHydra::TEntityKey<entityType>& id) const override; \
    entityType* Get ## entityName(const ::NYT::NHydra::TEntityKey<entityType>& id) const override; \
    const ::NYT::NHydra::TReadOnlyEntityMap<entityType>& entityNamePlural() const override

#define DECLARE_ENTITY_MAP_ACCESSORS(entityName, entityType) \
    DECLARE_ENTITY_MAP_ACCESSORS_IMPL(entityName, entityName ## s, entityType)

#define DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(entityName, entityType) \
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE_IMPL(entityName, entityName ## s, entityType)

#define DECLARE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS_OVERRIDE(entityName, entityNamePlural, entityType) \
    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE_IMPL(entityName, entityNamePlural, entityType)

#define DEFINE_ENTITY_MAP_ACCESSORS_IMPL(declaringType, entityName, entityNamePlural, entityType, map) \
    entityType* declaringType::Find ## entityName(const ::NYT::NHydra::TEntityKey<entityType>& id) const \
    { \
        return (map).Find(id); \
    } \
    \
    entityType* declaringType::Get ## entityName(const ::NYT::NHydra::TEntityKey<entityType>& id) const \
    { \
        return (map).Get(id); \
    } \
    \
    const ::NYT::NHydra::TReadOnlyEntityMap<entityType>& declaringType::entityNamePlural() const \
    { \
        return (map); \
    } \
    static_assert(true)

#define DEFINE_ENTITY_MAP_ACCESSORS(declaringType, entityName, entityType, map) \
    DEFINE_ENTITY_MAP_ACCESSORS_IMPL(declaringType, entityName, entityName ## s, entityType, map)

#define DEFINE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(declaringType, entityName, entityNamePlural, entityType, map) \
    DEFINE_ENTITY_MAP_ACCESSORS_IMPL(declaringType, entityName, entityNamePlural, entityType, map)

#define DELEGATE_ENTITY_MAP_ACCESSORS_IMPL(declaringType, entityName, entityNamePlural, entityType, delegateTo) \
    entityType* declaringType::Find ## entityName(const ::NYT::NHydra::TEntityKey<entityType>& id) const \
    { \
        return (delegateTo).Find ## entityName(id); \
    } \
    \
    entityType* declaringType::Get ## entityName(const ::NYT::NHydra::TEntityKey<entityType>& id) const \
    { \
        return (delegateTo).Get ## entityName(id); \
    } \
    \
    const ::NYT::NHydra::TReadOnlyEntityMap<entityType>& declaringType::entityNamePlural() const \
    { \
        return (delegateTo).entityNamePlural(); \
    } \
    static_assert(true)

#define DELEGATE_ENTITY_MAP_ACCESSORS(declaringType, entityName, entityType, delegateTo) \
    DELEGATE_ENTITY_MAP_ACCESSORS_IMPL(declaringType, entityName, entityName ## s, entityType, delegateTo)

#define DELEGATE_ENTITY_WITH_IRREGULAR_PLURAL_MAP_ACCESSORS(declaringType, entityName, entityNamePlural, entityType, delegateTo) \
    DELEGATE_ENTITY_MAP_ACCESSORS_IMPL(declaringType, entityName, entityNamePlural, entityType, delegateTo)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra

#define ENTITY_MAP_INL_H_
#include "entity_map-inl.h"
#undef ENTITY_MAP_INL_H_
