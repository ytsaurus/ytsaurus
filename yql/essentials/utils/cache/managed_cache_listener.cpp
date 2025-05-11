#include "managed_cache_listener.h"

namespace NYql {

    void IManagedCacheListener::OnHit() {
    }

    void IManagedCacheListener::OnMiss() {
    }

    void IManagedCacheListener::OnEvict(size_t /*count*/) {
    }

    void IManagedCacheListener::OnUpdate(size_t /*count*/) {
    }

    void IManagedCacheListener::OnTickSucceded(TDuration /*duration*/) {
    }

    void IManagedCacheListener::OnTickFailed(TDuration /*duration*/) {
    }

    IManagedCacheListener::TPtr MakeDummyManagedCacheListener() {
        return new IManagedCacheListener();
    }

} // namespace NYql
