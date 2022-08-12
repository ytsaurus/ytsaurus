#pragma once

#include "service_detail.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TPerUserRequestQueues
{
public:
    using TReconfigurationCallback = std::function<void(TString, TRequestQueuePtr)>;

    TPerUserRequestQueues();

    TPerUserRequestQueues(TReconfigurationCallback reconfigurationCallback);

    TRequestQueueProvider GetProvider();

    void EnableThrottling(bool enableWeightThrottling, bool enableBytesThrottling);

    void ReconfigureCustomUserThrottlers(const TString& userName);

    void ReconfigureDefaultUserThrottlers(const TRequestQueueThrottlerConfigs& configs);

private:
    TRequestQueuePtr GetOrCreateUserQueue(const TString& userName);

    NConcurrency::TSyncMap<TString, TRequestQueuePtr> RequestQueues_;

    TAtomicObject<TRequestQueueThrottlerConfigs> DefaultConfigs_;

    TReconfigurationCallback ReconfigurationCallback_;

    NThreading::TReaderWriterSpinLock ThrottlingEnabledFlagsSpinLock_;
    bool WeightThrottlingEnabled_ = false;
    bool BytesThrottlingEnabled_ = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
