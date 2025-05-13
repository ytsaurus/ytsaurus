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

    void IManagedCacheMaintainenceListener::OnTickFailed(std::exception_ptr e) {
        std::rethrow_exception(e);
    }

} // namespace NYql
