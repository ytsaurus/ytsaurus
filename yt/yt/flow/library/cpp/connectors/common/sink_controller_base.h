#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/sink_controller.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TSinkControllerBase
    : public virtual ISinkController
{
public:
    TSinkControllerBase(
        TSinkControllerContextPtr context,
        TDynamicSinkControllerContextPtr dynamicContext);

    TSinkControllerContextPtr GetContext() const;
    TDynamicSinkControllerContextPtr GetDynamicContext() const;
    TSinkSpecPtr GetSpec() const;
    TDynamicSinkSpecPtr GetDynamicSpec() const;

    void Init(IInitContextPtr initContext) override;
    void Sync() override;
    void Commit() override;

    void UpdateWatermarkState(TWatermarkStatePtr watermarkState) final;
    TWatermarkStatePtr GetWatermarkState();

protected:
    const NLogging::TLogger Logger;

    ISink::TParametersPtr GetParametersBase() const final;
    ISink::TDynamicParametersPtr GetDynamicParametersBase() const final;

private:
    const TSinkControllerContextPtr Context_;
    const ISink::TParametersPtr Parameters_;
    TAtomicIntrusivePtr<TDynamicSinkControllerContext> DynamicContext_;
    TAtomicIntrusivePtr<ISink::TDynamicParameters> DynamicParameters_;
    TAtomicIntrusivePtr<TWatermarkState> WatermarkState_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
