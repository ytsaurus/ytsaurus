#pragma once

#include "private.h"

#include "data_flow_graph.h"

#include <yt/server/lib/chunk_pools/chunk_stripe_key.h>

#include <yt/server/lib/controller_agent/serialize.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/ytlib/table_client/helpers.h>
#include <yt/ytlib/table_client/samples_fetcher.h>

#include <yt/ytlib/scheduler/config.h>

namespace NYT::NControllerAgent {

using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(NYTree::INodePtr specNode);

NYTree::INodePtr UpdateSpec(NYTree::INodePtr templateSpec, NYTree::INodePtr originalSpec);

////////////////////////////////////////////////////////////////////////////////

TString TrimCommandForBriefSpec(const TString& command);

////////////////////////////////////////////////////////////////////////////////

struct TUserFile
    : public NChunkClient::TUserObject
{
    TUserFile() = default;
    TUserFile(
        NYPath::TRichYPath path,
        std::optional<NObjectClient::TTransactionId> transactionId,
        bool layer);

    std::shared_ptr<NYTree::IAttributeDictionary> Attributes;
    TString FileName;
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
    bool Executable = false;
    NYson::TYsonString Format;
    NTableClient::TTableSchema Schema;
    bool Dynamic = false;
    bool Layer = false;
    // This field is used only during file size validation only for table chunks with column selectors.
    std::vector<NChunkClient::TInputChunkPtr> Chunks;

    void Persist(const TPersistenceContext& context);
};

////////////////////////////////////////////////////////////////////////////////

NChunkPools::TBoundaryKeys BuildBoundaryKeysFromOutputResult(
    const NScheduler::NProto::TOutputResult& boundaryKeys,
    const TEdgeDescriptor& outputTable,
    const NTableClient::TRowBufferPtr& rowBuffer);

void BuildFileSpecs(NScheduler::NProto::TUserJobSpec* jobSpec, const std::vector<TUserFile>& files);

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TDataSourceDirectoryPtr BuildDataSourceDirectoryFromInputTables(const std::vector<TInputTablePtr>& inputTables);
NChunkClient::TDataSourceDirectoryPtr BuildIntermediateDataSourceDirectory();

void SetDataSourceDirectory(NScheduler::NProto::TSchedulerJobSpecExt* jobSpec, const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory);

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TAvgSummary
{
public:
    DEFINE_BYVAL_RO_PROPERTY(T, Sum);
    DEFINE_BYVAL_RO_PROPERTY(i64, Count);
    DEFINE_BYVAL_RO_PROPERTY(std::optional<T>, Avg);

public:
    TAvgSummary();
    TAvgSummary(T sum, i64 count);

    void AddSample(T sample);

    void Persist(const TPersistenceContext& context);

private:
    std::optional<T> CalcAvg();
};

////////////////////////////////////////////////////////////////////////////////

ELegacyLivePreviewMode ToLegacyLivePreviewMode(std::optional<bool> enableLegacyLivePreview);

////////////////////////////////////////////////////////////////////////////////

struct TPartitionKey
{
    NTableClient::TKey Key;

    //! Whether partition starting with this key is maniac.
    bool Maniac = false;

    TPartitionKey() = default;

    explicit TPartitionKey(NTableClient::TKey key)
        : Key(std::move(key))
    { }
};

////////////////////////////////////////////////////////////////////////////////

std::vector<const NTableClient::TSample*> SortSamples(const std::vector<NTableClient::TSample>& samples);

std::vector<TPartitionKey> BuildPartitionKeysBySamples(
    const std::vector<NTableClient::TSample>& samples,
    int partitionCount,
    const IJobSizeConstraintsPtr& partitionJobSizeConstraints,
    int keyPrefixLength,
    const NQueryClient::TRowBufferPtr& rowBuffer);

////////////////////////////////////////////////////////////////////////////////

//! Returns a future that is either set to an actual value (if the original one is set in timely manner)
//! or to |std::nullopt| (in case of timeout).
//! Moreover, in case of timeout |onFinishedAfterTimeout| callback will be subscribed to the original future.
//! The original future is NOT cancelled on timeout or when this future is cancelled.
template <class T>
TFuture<std::optional<T>> WithSoftTimeout(
    TFuture<T> future,
    TDuration timeout,
    TCallback<void(const TErrorOr<T>&)> onFinishedAfterTimeout = {});

////////////////////////////////////////////////////////////////////////////////

TDiskQuota CreateDiskQuota(
    const TDiskRequestConfigPtr& diskRequestConfig,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
