#pragma once

#ifndef ORDERED_MEMORY_INL_H_
    #error "Direct inclusion of this file is not allowed, include ordered_memory.h"
    // For the sake of sane code completion.
    #include "ordered_memory.h"
#endif

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TKeyedState>
void TOrderedMemoryState<TKeyedState>::Register(TRegistrar registrar)
{
    registrar.Parameter("inflight_keys", &TThis::InflightKeys)
        .Default();
}

template <class TKey, class TStateHolder>
bool TOrderedMemory<TKey, TStateHolder>::IsRegistered(const TKey& key)
{
    return !this->InflightKeys.empty() && key <= this->InflightKeys.back().first;
}

template <class TKey, class TStateHolder>
void TOrderedMemory<TKey, TStateHolder>::Register(const TKey& key, const TStateHolder& state)
{
    if (auto index = FindIndex(key); index == std::ssize(this->InflightKeys)) {
        if (!this->InflightKeys.empty() && this->InflightKeys.back().second == state) {
            this->InflightKeys.back().first = key;
        } else {
            this->InflightKeys.push_back(std::pair{key, state});
        }
    }
}

template <class TKey, class TStateHolder>
void TOrderedMemory<TKey, TStateHolder>::AdvanceExclusive(const TKey& keyExclusive)
{
    while (!this->InflightKeys.empty() && this->InflightKeys.front().first < keyExclusive) {
        this->InflightKeys.pop_front();
    }
}

template <class TKey, class TStateHolder>
const TStateHolder& TOrderedMemory<TKey, TStateHolder>::Extract(const TKey& key) const
{
    auto index = FindIndex(key);
    YT_VERIFY(index < std::ssize(this->InflightKeys), "Attempt to extract key that is not registered yet (or TOrderedMemory is empty)");
    return this->InflightKeys[index].second;
}

template <class TKey, class TStateHolder>
int TOrderedMemory<TKey, TStateHolder>::FindIndex(const TKey& key) const
{
    auto iter = std::lower_bound(this->InflightKeys.begin(), this->InflightKeys.end(), key, [] (const auto& element, const auto& key) {
        return element.first < key;
    });
    auto index = iter - this->InflightKeys.begin();
    return index;
}

template <class TKey, class TStateHolder>
void TOrderedMemory<TKey, TStateHolder>::clear()
{
    this->InflightKeys.clear();
}

template <class TKey, class TStateHolder>
bool TOrderedMemory<TKey, TStateHolder>::empty() const
{
    return size() == 0;
}

template <class TKey, class TStateHolder>
int TOrderedMemory<TKey, TStateHolder>::size() const
{
    return std::size(this->InflightKeys);
}

template <class TKey, class TStateHolder>
const std::pair<TKey, TStateHolder>& TOrderedMemory<TKey, TStateHolder>::front() const
{
    return this->InflightKeys.front();
}

template <class TKey, class TStateHolder>
const std::pair<TKey, TStateHolder>& TOrderedMemory<TKey, TStateHolder>::back() const
{
    return this->InflightKeys.back();
}

template <class TKey, class TStateHolder>
std::deque<std::pair<TKey, TStateHolder>>::const_iterator TOrderedMemory<TKey, TStateHolder>::begin() const
{
    return this->InflightKeys.begin();
}

template <class TKey, class TStateHolder>
std::deque<std::pair<TKey, TStateHolder>>::const_iterator TOrderedMemory<TKey, TStateHolder>::end() const
{
    return this->InflightKeys.end();
}

template <class TKey, class TStateHolder>
void TOrderedMemory<TKey, TStateHolder>::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
