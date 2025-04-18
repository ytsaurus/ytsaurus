#pragma once

#include "request_discriminator.h"

#include <contrib/ydb/library/actors/wilson/wilson_trace.h>

namespace NKikimr::NJaegerTracing {

class TSamplingThrottlingControl: public TThrRefBase {
    friend class TSamplingThrottlingConfigurator;
    
public:
    NWilson::TTraceId HandleTracing(const TRequestDiscriminator& discriminator,
            const TMaybe<TString>& traceparent);

    ~TSamplingThrottlingControl();
    
private:
    struct TSamplingThrottlingImpl;

    // Should only be obtained from TSamplingThrottlingConfigurator
    TSamplingThrottlingControl(std::unique_ptr<TSamplingThrottlingImpl> initialImpl);

    void UpdateImpl(std::unique_ptr<TSamplingThrottlingImpl> newParams);

    // Exclusively owned by the only thread, that may call HandleTracing
    std::unique_ptr<TSamplingThrottlingImpl> Impl;

    // Shared between the thread calling HandleTracing and the thread calling UpdateParams
    std::atomic<TSamplingThrottlingImpl*> ImplUpdate{nullptr};
};

} // namespace NKikimr::NJaegerTracing
