#pragma once

#include "private.h"

#include "data_flow_graph.h"

#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe_key.h>

#include <yt/yt/ytlib/table_client/helpers.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

NChunkPools::TBoundaryKeys BuildBoundaryKeysFromOutputResult(
    const NScheduler::NProto::TOutputResult& boundaryKeys,
    const TOutputStreamDescriptorPtr& outputTable,
    const NTableClient::TRowBufferPtr& rowBuffer);

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TDataSourceDirectoryPtr BuildDataSourceDirectoryFromInputTables(const std::vector<TInputTablePtr>& inputTables);
NChunkClient::TDataSinkDirectoryPtr BuildDataSinkDirectoryFromOutputTables(const std::vector<TOutputTablePtr>& outputTables);

NChunkClient::TDataSinkDirectoryPtr BuildDataSinkDirectoryWithAutoMerge(
    const std::vector<TOutputTablePtr>& outputTables,
    const std::vector<bool>& autoMergeEnabled,
    const std::optional<TString>& intermediateAccountName);

NChunkClient::TDataSink BuildDataSinkFromOutputTable(const TOutputTablePtr& outputTable);

std::vector<TInputStreamDescriptorPtr> BuildInputStreamDescriptorsFromOutputStreamDescriptors(
    const std::vector<TOutputStreamDescriptorPtr>& outputStreamDescriptors);

////////////////////////////////////////////////////////////////////////////////

class TControllerFeatures
{
public:
    void AddTag(TString name, auto value);
    void AddSingular(TStringBuf name, double value);
    void AddSingular(const TString& name, const NYTree::INodePtr& node);
    void AddCounted(TStringBuf name, double value);
    void CalculateJobSatisticsAverage();

private:
    THashMap<TString, NYson::TYsonString> Tags_;
    THashMap<TString, double> Features_;

    friend void Serialize(const TControllerFeatures& features, NYson::IYsonConsumer* consumer);
};

void Serialize(const TControllerFeatures& features, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableReaderOptionsPtr CreateTableReaderOptions(const NScheduler::TJobIOConfigPtr& ioConfig);

////////////////////////////////////////////////////////////////////////////////

void UpdateAggregatedJobStatistics(
    TAggregatedJobStatistics& targetStatistics,
    const TJobStatisticsTags& tags,
    const TStatistics& jobStatistics,
    const TStatistics& controllerStatistics,
    int customStatisticsLimit,
    bool* isLimitExceeded);

////////////////////////////////////////////////////////////////////////////////

struct TDockerImageSpec
{
    TString Registry;
    TString Image;
    TString Tag;

    TDockerImageSpec(const TString& dockerImage, const TDockerRegistryConfigPtr& config);

    bool IsInternal() const;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<NYPath::TRichYPath> GetLayerPathsFromDockerImage(
    NApi::NNative::IClientPtr client,
    const TDockerImageSpec& dockerImage);

////////////////////////////////////////////////////////////////////////////////

NYTree::IAttributeDictionaryPtr GetNetworkProject(
    NApi::NNative::IClientPtr client,
    TString authenticatedUser,
    TString networkProject);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

#define HELPERS_INL_H
#include "helpers-inl.h"
#undef HELPERS_INL_H
