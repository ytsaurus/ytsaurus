#pragma once

#include <util/datetime/base.h>
#include <util/generic/ptr.h>

namespace NYql {

    class IManagedCacheListener: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IManagedCacheListener>;

        IManagedCacheListener() = default;
        ~IManagedCacheListener() = default;

        virtual void OnHit();
        virtual void OnMiss();
        virtual void OnEvict(size_t count);
        virtual void OnUpdate(size_t count);
        virtual void OnTickSucceded(TDuration duration);
        virtual void OnTickFailed(TDuration duration);
    };

    IManagedCacheListener::TPtr MakeDummyManagedCacheListener();

} // namespace NYql
