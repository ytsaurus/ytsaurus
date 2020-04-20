#include "helpers.h"
#include "config.h"

#include "table.h"

#include <yt/server/lib/scheduler/config.h>

#include <yt/server/lib/controller_agent/serialize.h>

#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/ytlib/scheduler/proto/output_result.pb.h>
#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/client/security_client/acl.h>

#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/hive/cluster_directory.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/table_client/row_buffer.h>

#include <yt/client/transaction_client/public.h>

#include <yt/core/ytree/helpers.h>

namespace NYT::NControllerAgent {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NSecurityClient;
using namespace NScheduler;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NApi;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TString TrimCommandForBriefSpec(const TString& command)
{
    const int MaxBriefSpecCommandLength = 256;
    return
        command.length() <= MaxBriefSpecCommandLength
        ? command
        : command.substr(0, MaxBriefSpecCommandLength) + "...";
}

////////////////////////////////////////////////////////////////////////////////

NYTree::INodePtr UpdateSpec(NYTree::INodePtr templateSpec, NYTree::INodePtr originalSpec)
{
    if (!templateSpec) {
        return originalSpec;
    }
    return PatchNode(templateSpec, originalSpec);
}

////////////////////////////////////////////////////////////////////////////////

TUserFile::TUserFile(
    NYPath::TRichYPath path,
    std::optional<TTransactionId> transactionId,
    bool layer)
    : TUserObject(std::move(path), transactionId)
    , Layer(layer)
{ }

void TUserFile::Persist(const TPersistenceContext& context)
{
    TUserObject::Persist(context);

    using NYT::Persist;
    Persist<TAttributeDictionaryRefSerializer>(context, Attributes);
    Persist(context, FileName);
    Persist(context, ChunkSpecs);
    Persist(context, ChunkCount);
    Persist(context, Type);
    Persist(context, Executable);
    Persist(context, Format);
    Persist(context, Schema);
    Persist(context, Dynamic);
    Persist(context, Layer);
}

////////////////////////////////////////////////////////////////////////////////

TBoundaryKeys BuildBoundaryKeysFromOutputResult(
    const NScheduler::NProto::TOutputResult& boundaryKeys,
    const TEdgeDescriptor& edgeDescriptor,
    const TRowBufferPtr& rowBuffer)
{
    YT_VERIFY(!boundaryKeys.empty());
    YT_VERIFY(boundaryKeys.sorted());
    YT_VERIFY(!edgeDescriptor.TableWriterOptions->ValidateUniqueKeys || boundaryKeys.unique_keys());

    auto trimAndCaptureKey = [&] (const TOwningKey& key) {
        int limit = edgeDescriptor.TableUploadOptions.TableSchema.GetKeyColumnCount();
        if (key.GetCount() > limit) {
            // NB: This can happen for a teleported chunk from a table with a wider key in sorted (but not unique_keys) mode.
            YT_VERIFY(!edgeDescriptor.TableWriterOptions->ValidateUniqueKeys);
            return rowBuffer->Capture(key.Begin(), limit);
        } else {
            return rowBuffer->Capture(key.Begin(), key.GetCount());
        }
    };

    return TBoundaryKeys {
        trimAndCaptureKey(FromProto<TOwningKey>(boundaryKeys.min())),
        trimAndCaptureKey(FromProto<TOwningKey>(boundaryKeys.max())),
    };
}

void BuildFileSpecs(NScheduler::NProto::TUserJobSpec* jobSpec, const std::vector<TUserFile>& files)
{
    for (const auto& file : files) {
        auto* descriptor = file.Layer
            ? jobSpec->add_layers()
            : jobSpec->add_files();

        ToProto(descriptor->mutable_chunk_specs(), file.ChunkSpecs);

        if (file.Type == EObjectType::Table && file.Dynamic && file.Schema.IsSorted()) {
            auto dataSource = MakeVersionedDataSource(
                file.Path.GetPath(),
                file.Schema,
                file.Path.GetColumns(),
                file.OmittedInaccessibleColumns,
                file.Path.GetTimestamp().value_or(AsyncLastCommittedTimestamp),
                file.Path.GetRetentionTimestamp().value_or(NullTimestamp),
                file.Path.GetColumnRenameDescriptors().value_or(TColumnRenameDescriptors()));

            ToProto(descriptor->mutable_data_source(), dataSource);
        } else {
            auto dataSource = file.Type == EObjectType::File
                ? MakeFileDataSource(file.Path.GetPath())
                : MakeUnversionedDataSource(
                    file.Path.GetPath(),
                    file.Schema,
                    file.Path.GetColumns(),
                    file.OmittedInaccessibleColumns,
                    file.Path.GetColumnRenameDescriptors().value_or(TColumnRenameDescriptors()));

            ToProto(descriptor->mutable_data_source(), dataSource);
        }

        if (!file.Layer) {
            descriptor->set_file_name(file.FileName);
            descriptor->set_bypass_artifact_cache(file.Path.GetBypassArtifactCache());
            switch (file.Type) {
                case EObjectType::File:
                    descriptor->set_executable(file.Executable);
                    break;
                case EObjectType::Table:
                    descriptor->set_format(file.Format.GetData());
                    break;
                default:
                    YT_ABORT();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TDataSourceDirectoryPtr BuildDataSourceDirectoryFromInputTables(const std::vector<TInputTablePtr>& inputTables)
{
    auto dataSourceDirectory = New<TDataSourceDirectory>();
    for (const auto& inputTable : inputTables) {
        auto dataSource = (inputTable->Dynamic && inputTable->Schema.IsSorted())
            ? MakeVersionedDataSource(
                inputTable->GetPath(),
                inputTable->Schema,
                inputTable->Path.GetColumns(),
                inputTable->OmittedInaccessibleColumns,
                inputTable->Path.GetTimestamp().value_or(AsyncLastCommittedTimestamp),
                inputTable->Path.GetRetentionTimestamp().value_or(NullTimestamp),
                inputTable->ColumnRenameDescriptors)
            : MakeUnversionedDataSource(
                inputTable->GetPath(),
                inputTable->Schema,
                inputTable->Path.GetColumns(),
                inputTable->OmittedInaccessibleColumns,
                inputTable->ColumnRenameDescriptors);

        dataSource.SetForeign(inputTable->IsForeign());
        dataSourceDirectory->DataSources().push_back(dataSource);
    }

    return dataSourceDirectory;
}

TDataSourceDirectoryPtr BuildIntermediateDataSourceDirectory()
{
    auto dataSourceDirectory = New<TDataSourceDirectory>();
    dataSourceDirectory->DataSources().push_back(MakeUnversionedDataSource(
        IntermediatePath,
        /* schema */ std::nullopt,
        /* columns */ std::nullopt,
        /* omittedInaccessibleColumns */ {}));
    return dataSourceDirectory;
}

void SetDataSourceDirectory(
    NScheduler::NProto::TSchedulerJobSpecExt* jobSpec,
    const TDataSourceDirectoryPtr& dataSourceDirectory)
{
    NChunkClient::NProto::TDataSourceDirectoryExt dataSourceDirectoryExt;
    ToProto(&dataSourceDirectoryExt, dataSourceDirectory);
    SetProtoExtension(jobSpec->mutable_extensions(), dataSourceDirectoryExt);
}

////////////////////////////////////////////////////////////////////////////////

ELegacyLivePreviewMode ToLegacyLivePreviewMode(std::optional<bool> enableLegacyLivePreview)
{
    if (enableLegacyLivePreview) {
        return *enableLegacyLivePreview
            ? ELegacyLivePreviewMode::ExplicitlyEnabled
            : ELegacyLivePreviewMode::ExplicitlyDisabled;
    } else {
        return ELegacyLivePreviewMode::DoNotCare;
    }
}

////////////////////////////////////////////////////////////////////////////////

std::vector<const TSample*> SortSamples(const std::vector<TSample>& samples)
{
    int sampleCount = static_cast<int>(samples.size());

    std::vector<const TSample*> sortedSamples;
    sortedSamples.reserve(sampleCount);
    try {
        for (const auto& sample : samples) {
            ValidateClientKey(sample.Key);
            sortedSamples.push_back(&sample);
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Error validating table samples") << ex;
    }

    std::sort(
        sortedSamples.begin(),
        sortedSamples.end(),
        [] (const TSample* lhs, const TSample* rhs) {
            return *lhs < *rhs;
        });

    return sortedSamples;
}

std::vector<TPartitionKey> BuildPartitionKeysBySamples(
    const std::vector<TSample>& samples,
    int partitionCount,
    const IJobSizeConstraintsPtr& partitionJobSizeConstraints,
    int keyPrefixLength,
    const TRowBufferPtr& rowBuffer)
{
    YT_VERIFY(partitionCount > 0);

    auto sortedSamples = SortSamples(samples);

    std::vector<TPartitionKey> partitionKeys;

    i64 totalSamplesWeight = 0;
    for (const auto* sample : sortedSamples) {
        totalSamplesWeight += sample->Weight;
    }

    // Select samples evenly wrt weights.
    std::vector<const TSample*> selectedSamples;
    selectedSamples.reserve(partitionCount - 1);

    double weightPerPartition = (double)totalSamplesWeight / partitionCount;
    i64 processedWeight = 0;
    for (const auto* sample : sortedSamples) {
        processedWeight += sample->Weight;
        if (processedWeight / weightPerPartition > selectedSamples.size() + 1) {
            selectedSamples.push_back(sample);
        }
        if (selectedSamples.size() == partitionCount - 1) {
            // We need exactly partitionCount - 1 partition keys.
            break;
        }
    }

    // Invariant:
    //   lastKey = partitionsKeys.back().Key
    //   lastKey corresponds to partition receiving keys in [lastKey, ...)
    //
    // Initially partitionKeys is empty so lastKey is assumed to be -inf.

    int sampleIndex = 0;
    while (sampleIndex < selectedSamples.size()) {
        TKey lastKey = MinKey();
        if (!partitionKeys.empty()) {
            lastKey = partitionKeys.back().Key;
        }

        auto* sample = selectedSamples[sampleIndex];
        // Check for same keys.
        if (CompareRows(sample->Key, lastKey) != 0) {
            partitionKeys.emplace_back(rowBuffer->Capture(sample->Key));
            ++sampleIndex;
        } else {
            // Skip same keys.
            int skippedCount = 0;
            while (sampleIndex < selectedSamples.size() &&
                CompareRows(selectedSamples[sampleIndex]->Key, lastKey) == 0)
            {
                ++sampleIndex;
                ++skippedCount;
            }

            auto* lastManiacSample = selectedSamples[sampleIndex - 1];

            if (!lastManiacSample->Incomplete) {
                partitionKeys.back().Maniac = true;
                YT_VERIFY(skippedCount >= 1);

                // NB: in partitioner we compare keys with the whole rows,
                // so key prefix successor in required here.
                auto successorKey = GetKeyPrefixSuccessor(sample->Key, keyPrefixLength, rowBuffer);
                partitionKeys.emplace_back(successorKey);
            } else {
                // If sample keys are incomplete, we cannot use UnorderedMerge,
                // because full keys may be different.
                partitionKeys.emplace_back(rowBuffer->Capture(selectedSamples[sampleIndex]->Key));
                ++sampleIndex;
            }
        }
    }
    return partitionKeys;
}

////////////////////////////////////////////////////////////////////////////////

TDiskQuota CreateDiskQuota(
    const TDiskRequestConfigPtr& diskRequestConfig,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
{
    if (!diskRequestConfig->MediumIndex) {
        auto* mediumDescriptor = mediumDirectory->FindByName(diskRequestConfig->MediumName);
        if (!mediumDescriptor) {
            THROW_ERROR_EXCEPTION("Unknown medium %Qv", diskRequestConfig->MediumName);
        }
        diskRequestConfig->MediumIndex = mediumDescriptor->Index;
    }
    return NScheduler::CreateDiskQuota(*diskRequestConfig->MediumIndex, diskRequestConfig->DiskSpace);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent

