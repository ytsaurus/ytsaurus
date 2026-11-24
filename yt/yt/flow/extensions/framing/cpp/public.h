#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/framing/format.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFramingFormat,
    ((Auto)           (0))
    ((Protoseq)       (1))
    ((Lenval)         (2))
    ((LightProtoseq)  (3))
);

static_assert(static_cast<i64>(NFraming::EFormat::Auto) == 0);
static_assert(static_cast<i64>(NFraming::EFormat::Protoseq) == 1);
static_assert(static_cast<i64>(NFraming::EFormat::Lenval) == 2);
static_assert(static_cast<i64>(NFraming::EFormat::LightProtoseq) == 3);

DECLARE_REFCOUNTED_STRUCT(TProtoSourceComputationCommonSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicProtoSourceComputationCommonSpec);
DECLARE_REFCOUNTED_STRUCT(TProtoSinkComputationCommonSpec);
DECLARE_REFCOUNTED_STRUCT(TDynamicProtoSinkComputationCommonSpec);

DECLARE_REFCOUNTED_CLASS(TProtoSinkComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
