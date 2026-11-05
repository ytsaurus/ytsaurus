#pragma once

#include <yt/yt/core/actions/future.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TKeyedState>
struct TOrderedMemoryState
    : public virtual NYTree::TYsonStruct
{
    std::deque<TKeyedState> InflightKeys;

    REGISTER_YSON_STRUCT(TOrderedMemoryState<TKeyedState>)

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

template <class TKey, class TStateHolder>
class TOrderedMemory
    : public TOrderedMemoryState<std::pair<TKey, TStateHolder>>
{
public:
    // Checks if key is registered in ordered memory. All keys <= maxRegisteredKey are assumed as registered.
    // If key was "advanced", result is undefined.
    bool IsRegistered(const TKey& key);

    void Register(const TKey& key, const TStateHolder& state);
    void AdvanceExclusive(const TKey& keyExclusive);
    const TStateHolder& Extract(const TKey& key) const;

    void clear();
    bool empty() const;
    int size() const;
    const std::pair<TKey, TStateHolder>& front() const;
    const std::pair<TKey, TStateHolder>& back() const;
    std::deque<std::pair<TKey, TStateHolder>>::const_iterator begin() const;
    std::deque<std::pair<TKey, TStateHolder>>::const_iterator end() const;

    TOrderedMemory()
    {
        static_assert(std::derived_from<TOrderedMemory<TKey, TStateHolder>, ::NYT::NYTree::TYsonStruct>, "Class must inherit from TYsonStruct");
        YSON_STRUCT_IMPL__CTOR_BODY
    }

private:
    using TRegistrar = ::NYT::NYTree::TYsonStructRegistrar<TOrderedMemory<TKey, TStateHolder>>;
    using TThis = TOrderedMemory<TKey, TStateHolder>;
    friend class ::NYT::NYTree::TYsonStructRegistry;

    static void Register(TRegistrar registrar);

private:
    int FindIndex(const TKey& key) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define ORDERED_MEMORY_INL_H_
#include "ordered_memory-inl.h"
#undef ORDERED_MEMORY_INL_H_
