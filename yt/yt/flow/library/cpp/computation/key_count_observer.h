#pragma once

#include <yt/yt/flow/library/cpp/common/key.h>

#include <library/cpp/yt/system/spin_lock.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TKeyCountObserver
{
public:
    void ObserveKey(const TKey& key, TInstant now);
    i64 GetKeyCount() const;
    THashMap<TKey, TInstant> GetKeys() const;

private:
    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    THashMap<TKey, TInstant> Keys_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
