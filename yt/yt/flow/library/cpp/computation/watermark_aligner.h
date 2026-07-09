#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct IWatermarkAligner
    : public virtual TRefCounted
{
    virtual bool IsAllowToRead(const TSystemTimestamp& localReadWatermark, TWatermarkStatePtr watermarkState) = 0;
};

DEFINE_REFCOUNTED_TYPE(IWatermarkAligner);

////////////////////////////////////////////////////////////////////////////////

IWatermarkAlignerPtr CreateWatermarkAligner(
    TWatermarkAlignmentSpecPtr spec,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
