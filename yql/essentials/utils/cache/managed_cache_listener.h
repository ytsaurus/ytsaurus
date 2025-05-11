#pragma once

#include <util/datetime/base.h>
#include <util/generic/ptr.h>

namespace NYql {

    class IManagedCacheStorageListener: public virtual TThrRefBase {
    public:
        IManagedCacheStorageListener() = default;
        ~IManagedCacheStorageListener() = default;
        virtual void OnHit();
        virtual void OnMiss();
        virtual void OnEvict(size_t count);
        virtual void OnUpdate(size_t count);
    };

    class IManagedCacheMaintainenceListener: public virtual TThrRefBase {
    public:
        IManagedCacheMaintainenceListener() = default;
        ~IManagedCacheMaintainenceListener() = default;
        virtual void OnTickBegin();
        virtual void OnTickSucceded();
        virtual void OnTickFailed(const std::exception& exception);
    };

    class IManagedCacheListener
        : public IManagedCacheStorageListener,
          public IManagedCacheMaintainenceListener,
          public virtual TThrRefBase {
    };

} // namespace NYql
