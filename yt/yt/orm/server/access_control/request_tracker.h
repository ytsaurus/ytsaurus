#pragma once

#include "public.h"

namespace NYT::NOrm::NServer::NAccessControl {

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

    TFuture<void> ThrottleUserRequest(std::string_view userId, i64 requestWeight, bool soft);

    void ReconfigureUsersBatch(const std::vector<TUserSetup>& update);
    bool TryIncreaseRequestQueueSize(std::string_view userId, int requestCount, bool soft);
    void DecreaseRequestQueueSize(std::string_view userId, int requestCount);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TRequestTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NAccessControl
