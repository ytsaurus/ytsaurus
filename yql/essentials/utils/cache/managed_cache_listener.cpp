#include "managed_cache_listener.h"

namespace NYql {

    void IManagedCacheStorageListener::OnHit() {
    }

    void IManagedCacheStorageListener::OnMiss() {
    }

    void IManagedCacheStorageListener::OnEvict(size_t /*count*/) {
    }

    void IManagedCacheStorageListener::OnUpdate(size_t /*count*/) {
    }

    void IManagedCacheMaintainenceListener::OnTickBegin() {
    }

    void IManagedCacheMaintainenceListener::OnTickSucceded() {
    }

    void IManagedCacheMaintainenceListener::OnTickFailed(const std::exception& /*exception*/) {
    }

} // namespace NYql
