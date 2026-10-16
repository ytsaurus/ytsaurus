#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/computation.h>
#include <yt/yt/flow/library/cpp/common/lag.h>
#include <yt/yt/flow/library/cpp/common/sink.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TSinkBase
    : public ISink
{
public:
    TSinkBase(
        TSinkContextPtr context,
        TDynamicSinkContextPtr dynamicContext);

    TSinkContextPtr GetContext() const;
    TDynamicSinkContextPtr GetDynamicContext() const;
    TSinkSpecPtr GetSpec() const;
    TDynamicSinkSpecPtr GetDynamicSpec() const;

protected:
    ISink::TParametersPtr GetParametersBase() const final;
    ISink::TDynamicParametersPtr GetDynamicParametersBase() const final;

    //! Records and flushes the now() - eventTimestamp lag for |streamId|.
    //! Called by each derived sink at its natural delivery moment.
    void ObserveEventLag(const TStreamId& streamId, TSystemTimestamp eventTimestamp);

protected:
    const NLogging::TLogger Logger;

private:
    const TSinkContextPtr Context_;
    const ISink::TParametersPtr Parameters_;
    TAtomicIntrusivePtr<TDynamicSinkContext> DynamicContext_;
    TAtomicIntrusivePtr<ISink::TDynamicParameters> DynamicParameters_;
    TStreamEventLagObserver EventLagObserver_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
