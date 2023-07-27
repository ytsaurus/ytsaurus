#pragma once

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobTestingOptions
    : public NYTree::TYsonStruct
{
public:
    std::optional<TDuration> DelayAfterNodeDirectoryPrepared;
    std::optional<TDuration> DelayInCleanup;
    std::optional<TDuration> DelayBeforeRunJobProxy;
    std::optional<TDuration> DelayBeforeSpawningJobProxy;
    std::optional<TDuration> DelayAfterRunJobProxy;
    bool FailBeforeJobStart;
    bool ThrowInShallowMerge;

    REGISTER_YSON_STRUCT(TJobTestingOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TJobTestingOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
