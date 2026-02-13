#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBreakpointType,
    ((BeforeRun)  (0))
);

////////////////////////////////////////////////////////////////////////////////

struct TEventsOnFsConfig
    : public NYTree::TYsonStruct
{
    std::string Path;
    THashSet<EBreakpointType> Breakpoints;
    TDuration Timeout;
    TDuration PollPeriod;

    REGISTER_YSON_STRUCT(TEventsOnFsConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TEventsOnFsConfig)

////////////////////////////////////////////////////////////////////////////////

struct TJobTestingOptions
    : public NYTree::TYsonStruct
{
    std::optional<TDuration> DelayAfterNodeDirectoryPrepared;
    std::optional<TDuration> DelayInCleanup;
    std::optional<TDuration> DelayBeforeRunJobProxy;
    std::optional<TDuration> DelayBeforeSpawningJobProxy;
    std::optional<TDuration> DelayAfterRunJobProxy;
    std::optional<TDuration> FakePrepareDuration;
    bool FailBeforeJobStart;
    bool ThrowInShallowMerge;

    TEventsOnFsConfigPtr EventsOnFs;

    REGISTER_YSON_STRUCT(TJobTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobTestingOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
