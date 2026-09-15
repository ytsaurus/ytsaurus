#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <util/generic/strbuf.h>

namespace NYT::NTCMalloc {

////////////////////////////////////////////////////////////////////////////////

inline constexpr TStringBuf CurrentMemoryProfileFilePrefix = "current_";
inline constexpr TStringBuf PeakMemoryProfileFilePrefix = "peak_";
inline constexpr TStringBuf OomMemoryProfileManifestFilePrefix = "oom_profile_paths_";

inline constexpr TStringBuf MemoryProfileFileExtension = ".pb.gz";
inline constexpr TStringBuf IncompleteMemoryProfileFileExtension = ".pb.gz_incomplete";
inline constexpr TStringBuf OomMemoryProfileManifestFileExtension = ".yson";
inline constexpr TStringBuf IncompleteMemoryProfileFileSuffix = "_incomplete";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TTCMallocConfig)
DECLARE_REFCOUNTED_STRUCT(THeapSizeLimitConfig)
DECLARE_REFCOUNTED_STRUCT(TMemoryProfileRetentionConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicTCMallocConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicHeapSizeLimitConfig)

YT_DECLARE_RECONFIGURABLE_SINGLETON(TTCMallocConfig, TDynamicTCMallocConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTCMalloc
