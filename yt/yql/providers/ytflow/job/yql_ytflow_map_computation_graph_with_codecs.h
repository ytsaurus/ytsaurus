#pragma once

#include "yql_ytflow_computation_graph_with_codecs_base.h"
#include "yql_ytflow_message_holder.h"
#include "yql_ytflow_stream_value.h"
#include "yql_ytflow_utils.h"

#include <yql/essentials/public/langver/yql_langver.h>
#include <yql/essentials/minikql/runtime_settings/runtime_settings.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/payload_converter.h>
#include <yt/yt/flow/library/cpp/common/public.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <vector>


namespace NYql::NYtflow {

TComputationGraphResources ResolveMapComputationGraphResources(
    const THashMap<NYT::NFlow::TResourceId, NYT::NFlow::IResourcePtr>& staticResources);

class IMapComputationGraphWithCodecs
{
public:
    virtual ~IMapComputationGraphWithCodecs() = default;

public:
    virtual void SetInput(
        const TMessageHolder& messageHolder) = 0;

    virtual void SetInput(
        const std::vector<TMessageHolder>& messageHolders) = 0;

    virtual bool FetchOutput(TVector<NYT::NFlow::TMessage>& messages) = 0;

    virtual void ResetInput() = 0;
};

THolder<IMapComputationGraphWithCodecs> CreateMapComputationGraphWithCodecs(
    TString lambdaFile,
    NYT::NTableClient::TTableSchemaPtr inputSchema,
    THashMap<ui32, TVector<TOutputStreamInfo>> outputStreamInfosByOutputIndex,
    TVector<TString> udfPaths,
    EInputMode inputMode,
    TLangVersion langVersion,
    TString optLLVM,
    TRuntimeSettings::TConstPtr runtimeSettings,
    bool injectInputMessageId,
    NYT::NProfiling::TProfiler profiler,
    NYT::NFlow::IPayloadConverterCachePtr converterCache,
    TComputationGraphResources resources = {});

} // namespace NYql::NYtflow
