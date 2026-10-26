#include "key_count_observer.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TKeyCountObserver::ObserveKey(const TKey& key, TInstant now)
{
    auto guard = Guard(Lock_);
    auto [it, inserted] = Keys_.try_emplace(key, now);
    it->second = now;
}

i64 TKeyCountObserver::GetKeyCount() const
{
    auto guard = Guard(Lock_);
    return std::ssize(Keys_);
}

THashMap<TKey, TInstant> TKeyCountObserver::GetKeys() const
{
    auto guard = Guard(Lock_);
    return Keys_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
