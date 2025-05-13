#include "managed_cache_listener.h"

#include <yql/essentials/utils/log/log.h>

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

    void IManagedCacheMaintainenceListener::OnTickFailed(std::exception_ptr) {
        YQL_LOG(ERROR) << "Cache Maintainence Tick failed: " << CurrentExceptionMessage();
    }

} // namespace NYql
