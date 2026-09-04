#pragma once

#include "yql_ytflow_computation_graph_with_codecs_base.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/flow/library/cpp/common/public.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>


namespace NYql::NYtflow {

struct TPostprocessOutput {
    TVector<NYT::NFlow::TMessage> Messages;
    TString State;
    bool CleanupState;
};


class IPostprocessComputationGraphWithCodecs {
public:
    virtual ~IPostprocessComputationGraphWithCodecs() = default;

public:
    virtual void SetInput(
        const NYT::NFlow::TKey& key,
        TString state,
        ui64 maxHopStartTime) = 0;

    virtual TPostprocessOutput GetOutput() = 0;

    virtual void ResetInput() = 0;
};


THolder<IPostprocessComputationGraphWithCodecs> CreatePostprocessComputationGraphWithCodecs(
    TString lambdaFile,
    TVector<TOutputStreamInfo> outputStreamInfos,
    TVector<TString> udfPaths,
    TLangVersion langVersion,
    TString optLLVM,
    NYql::TRuntimeSettings::TConstPtr runtimeSettings,
    NYT::NProfiling::TProfiler profiler,
    NYT::NFlow::IPayloadConverterCachePtr converterCache,
    TComputationGraphResources resources = {});

} // namespace NYql::NYtflow
