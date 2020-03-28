#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

namespace NYP::NServer::NAccessControl {

////////////////////////////////////////////////////////////////////////////////

class TRequestTracker
    : public TRefCounted
{
public:
    struct TUserSetup
    {
        NObjects::TObjectId UserId;
        i64 RequestWeightRateLimit = 0;
        int RequestQueueSizeLimit = 0;

        TUserSetup(NObjects::TObjectId userId, i64 requestWeightRateLimit, int requestQueueSizeLimit);
    };

    explicit TRequestTracker(TRequestTrackerConfigPtr config);
    ~TRequestTracker();


    TFuture<void> ThrottleUserRequest(
        const NObjects::TObjectId& userId,
        i64 requestCount);

    void ReconfigureUsersBatch(const std::vector<TUserSetup>& update);
    bool TryIncreaseRequestQueueSize(const NObjects::TObjectId& userId, int requestCount);
    void DecreaseRequestQueueSize(const NObjects::TObjectId& userId, int requestCount);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TRequestTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NAccessControl
