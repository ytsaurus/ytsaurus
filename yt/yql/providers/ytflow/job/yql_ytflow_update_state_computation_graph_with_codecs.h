#pragma once

#include "yql_ytflow_computation_graph_with_codecs_base.h"
#include "yql_ytflow_stream_value.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/flow/library/cpp/common/public.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>
#include <vector>


namespace NYql::NYtflow {

struct TTimerInfo {
    ui64 TriggerTimestamp;
    ui64 EventTimestamp;
};


struct TUpdateStateOutput {
    TString State;
    TVector<TTimerInfo> TimerInfos;
};


class IUpdateStateComputationGraphWithCodecs {
public:
    virtual ~IUpdateStateComputationGraphWithCodecs() = default;

public:
    virtual void SetInput(
        const std::vector<TMessageHolder>& messageHolders,
        std::optional<TString> maybeState) = 0;

    virtual TUpdateStateOutput GetOutput() = 0;

    virtual void ResetInput() = 0;
};


THolder<IUpdateStateComputationGraphWithCodecs> CreateUpdateStateComputationGraphWithCodecs(
    TString lambdaFile,
    TVector<TString> udfPaths,
    TLangVersion langVersion,
    TString optLLVM,
    NYql::TRuntimeSettings::TConstPtr runtimeSettings,
    NYT::NTableClient::TTableSchemaPtr inputSchema,
    NYT::NProfiling::TProfiler profiler,
    NYT::NFlow::IPayloadConverterCachePtr converterCache,
    TComputationGraphResources resources = {});

} // namespace NYql::NYtflow
