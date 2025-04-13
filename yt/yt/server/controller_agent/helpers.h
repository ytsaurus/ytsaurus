#pragma once

#include "private.h"

#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/ytlib/table_client/samples_fetcher.h>

#include <yt/yt/library/query/base/public.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

template <class TSpec>
TIntrusivePtr<TSpec> ParseOperationSpec(NYTree::INodePtr specNode);

NYTree::INodePtr UpdateSpec(NYTree::INodePtr templateSpec, NYTree::INodePtr originalSpec);

template <class TOptions>
TOptions CreateOperationOptions(const TOptions& options, const NYTree::INodePtr& patch);

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

    NYTree::IAttributeDictionaryPtr Attributes;
    TString FileName;
    std::vector<NChunkClient::NProto::TChunkSpec> ChunkSpecs;
    bool Executable = false;
    NYson::TYsonString Format;
    NTableClient::TTableSchemaPtr Schema = New<NTableClient::TTableSchema>();
    bool Dynamic = false;
    bool Layer = false;
    bool GpuCheck = false;
    std::optional<ELayerAccessMethod> AccessMethod;
    std::optional<ELayerFilesystem> Filesystem;

    // This field is used only during file size validation only for table chunks with column selectors.
    std::vector<NChunkClient::TInputChunkPtr> Chunks;

    PHOENIX_DECLARE_TYPE(TUserFile, 0x3ba6ea91);
};

////////////////////////////////////////////////////////////////////////////////

void BuildFileSpec(
    NControllerAgent::NProto::TFileDescriptor* descriptor,
    const TUserFile& file,
    bool copyFiles,
    bool enableBypassArtifactCache);

////////////////////////////////////////////////////////////////////////////////

void BuildFileSpecs(
    NControllerAgent::NProto::TUserJobSpec* jobSpec,
    const std::vector<TUserFile>& files,
    const NScheduler::TUserJobSpecPtr& config,
    bool enableBypassArtifactCache);

////////////////////////////////////////////////////////////////////////////////

TString GetIntermediatePath(int streamIndex);

NChunkClient::TDataSourceDirectoryPtr BuildIntermediateDataSourceDirectory(
    const TString& intermediateAccount,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas = {});

NChunkClient::TDataSink BuildIntermediateDataSink(const TString& intermediateAccount);

NChunkClient::TDataSinkDirectoryPtr BuildIntermediateDataSinkDirectory(
    const TString& intermediateAccount);

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

private:
    std::optional<T> CalcAvg();

    PHOENIX_DECLARE_TEMPLATE_TYPE(TAvgSummary, 0x77455ee9);
};

////////////////////////////////////////////////////////////////////////////////

ELegacyLivePreviewMode ToLegacyLivePreviewMode(std::optional<bool> enableLegacyLivePreview);

////////////////////////////////////////////////////////////////////////////////

struct TPartitionKey
{
    NTableClient::TKeyBound LowerBound;

    //! Whether partition starting with this key is maniac.
    bool Maniac = false;

    TPartitionKey() = default;

    explicit TPartitionKey(NTableClient::TKeyBound lowerBound)
        : LowerBound(std::move(lowerBound))
    { }
};

////////////////////////////////////////////////////////////////////////////////

//! Constructs partition keys from the given samples.
//! The sample schema is used to fetch row values based on column names or expressions specified in the upload schema.
//! The sample schema must include all columns referenced in the key columns of the upload schema.
//! The upload schema is used to compare the sample rows.
//! It is guaranteed that the number of returned partition keys is less than partitionCount.
std::vector<TPartitionKey> BuildPartitionKeysFromSamples(
    const std::vector<NTableClient::TSample>& samples,
    const NTableClient::TTableSchemaPtr& sampleSchema,
    const NTableClient::TTableSchemaPtr& uploadSchema,
    const NQueryClient::IExpressionEvaluatorCachePtr& evaluatorCache,
    int partitionCount,
    const NTableClient::TRowBufferPtr& rowBuffer,
    const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

//! Describes shape of a partition tree.
struct TPartitionTreeSkeleton
{
    std::vector<std::unique_ptr<TPartitionTreeSkeleton>> Children;
};

std::unique_ptr<TPartitionTreeSkeleton> BuildPartitionTreeSkeleton(int partitionCount, int maxPartitionFactor);

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

NScheduler::TDiskQuota CreateDiskQuota(
    const NScheduler::TDiskRequestConfigPtr& diskRequestConfig,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
