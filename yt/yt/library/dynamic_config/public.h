#pragma once

#include <yt/yt/core/misc/error_code.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NDynamicConfig {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)
DECLARE_REFCOUNTED_CLASS(TDynamicConfigManagerConfig)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((FailedToFetchDynamicConfig)            (2600))
    ((DuplicateMatchingDynamicConfigs)       (2601))
    ((UnrecognizedDynamicConfigOption)       (2602))
    ((FailedToApplyDynamicConfig)            (2603))
    ((InvalidDynamicConfig)                  (2604))
    ((NoSuitableDynamicConfig)               (2605))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDynamicConfig
