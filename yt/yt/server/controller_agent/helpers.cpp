#include "helpers.h"
#include "config.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>

#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/client/security_client/acl.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/table_client/key_set.h>

#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/table_client/row_buffer.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/ytree/helpers.h>

#include <algorithm>

namespace NYT::NControllerAgent {

using namespace NObjectClient;
using namespace NChunkClient;
using namespace NChunkPools;
using namespace NSecurityClient;
using namespace NScheduler;
using namespace NTableClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;
using namespace NApi;
using namespace NLogging;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {
    void FromBytes(std::vector<TLegacyOwningKey>* keys, TStringBuf bytes)
{
    TKeySetReader reader(TSharedRef::FromString(TString(bytes)));
    for (auto key : reader.GetKeys()) {
        keys->push_back(TLegacyOwningKey(key));
    }
}

void ToBytes(TString* bytes, const std::vector<TLegacyOwningKey>& keys)
{
    auto keySetWriter = New<TKeySetWriter>();
    for (const auto& key : keys) {
        keySetWriter->WriteKey(key);
    }
    auto serializedKeys = keySetWriter->Finish();
    *bytes = TString(serializedKeys.begin(), serializedKeys.end());
}

// TODO(gritukan): Why does not it compile without these helpers?
void Serialize(const std::vector<TLegacyOwningKey>& keys, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .DoListFor(keys, [] (TFluentList fluent, const TLegacyOwningKey& key) {
            Serialize(key, fluent.GetConsumer());
        });
}

void Deserialize(std::vector<TLegacyOwningKey>& keys, INodePtr node)
{
    for (const auto& child : node->AsList()->GetChildren()) {
        TLegacyOwningKey key;
        Deserialize(key, child);
        keys.push_back(key);
    }
} // namespace

REGISTER_INTERMEDIATE_PROTO_INTEROP_BYTES_FIELD_REPRESENTATION(NProto::TPartitionJobSpecExt, /*wire_partition_keys*/8, std::vector<TLegacyOwningKey>)

}

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
    Persist<TAttributeDictionarySerializer>(context, Attributes);
    Persist(context, FileName);
    Persist(context, ChunkSpecs);
    Persist(context, Type);
    Persist(context, Executable);
    Persist(context, Format);
    Persist<TNonNullableIntrusivePtrSerializer<>>(context, Schema);
    Persist(context, Dynamic);
    Persist(context, Layer);

    // COMPAT(yuryalekseev)
    if (context.GetVersion() >= ESnapshotVersion::AddFilesystemAttribute) {
        Persist(context, Filesystem);
    }

    if (context.GetVersion() >= ESnapshotVersion::AddAccessMethodAttribute) {
        Persist(context, AccessMethod);
    }
}

////////////////////////////////////////////////////////////////////////////////

void BuildFileSpec(
    NControllerAgent::NProto::TFileDescriptor* descriptor,
    const TUserFile& file,
    bool copyFiles,
    bool enableBypassArtifactCache)
{
    ToProto(descriptor->mutable_chunk_specs(), file.ChunkSpecs);

    if (file.Type == EObjectType::Table && file.Dynamic && file.Schema->IsSorted()) {
        auto dataSource = MakeVersionedDataSource(
            file.Path.GetPath(),
            file.Schema,
            file.Path.GetColumns(),
            file.OmittedInaccessibleColumns,
            file.Path.GetTimestamp().value_or(AsyncLastCommittedTimestamp),
            file.Path.GetRetentionTimestamp().value_or(NullTimestamp),
            file.Path.GetColumnRenameDescriptors().value_or(TColumnRenameDescriptors()));
        dataSource.SetObjectId(file.ObjectId);
        dataSource.SetAccount(file.Account);

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
        dataSource.SetObjectId(file.ObjectId);
        dataSource.SetAccount(file.Account);

        ToProto(descriptor->mutable_data_source(), dataSource);
    }

    if (!file.Layer) {
        descriptor->set_file_name(file.FileName);
        if (enableBypassArtifactCache) {
            descriptor->set_bypass_artifact_cache(file.Path.GetBypassArtifactCache());
        }

        bool copyFile = file.Path.GetCopyFile().value_or(copyFiles);
        descriptor->set_copy_file(copyFile);

        switch (file.Type) {
            case EObjectType::File:
                descriptor->set_executable(file.Executable);
                descriptor->set_file_name(file.FileName);
                break;
            case EObjectType::Table:
                descriptor->set_format(file.Format.ToString());
                break;
            default:
                YT_ABORT();
        }
    }

    if (file.Layer) {
        if (file.AccessMethod) {
            descriptor->set_access_method(ToProto<int>(*file.AccessMethod));
        }

        if (file.Filesystem) {
            descriptor->set_filesystem(ToProto<int>(*file.Filesystem));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void BuildFileSpecs(
    NControllerAgent::NProto::TUserJobSpec* jobSpec,
    const std::vector<TUserFile>& files,
    const TUserJobSpecPtr& config,
    bool enableBypassArtifactCache)
{
    for (const auto& file : files) {
        auto* descriptor = file.Layer
            ? jobSpec->add_layers()
            : jobSpec->add_files();

        BuildFileSpec(descriptor, file, config->CopyFiles, enableBypassArtifactCache);
    }
}

////////////////////////////////////////////////////////////////////////////////

TString GetIntermediatePath(int streamIndex)
{
    return Format("<intermediate_%d>", streamIndex);
}

TDataSourceDirectoryPtr BuildIntermediateDataSourceDirectory(
    const TString& intermediateAccount,
    const std::vector<NTableClient::TTableSchemaPtr>& schemas)
{
    auto dataSourceDirectory = New<TDataSourceDirectory>();
    if (schemas.empty()) {
        dataSourceDirectory->DataSources().emplace_back(MakeUnversionedDataSource(
            GetIntermediatePath(0),
            New<TTableSchema>(),
            /*columns*/ std::nullopt,
            /*omittedInaccessibleColumns*/ {},
            /*columnRenameDescriptors*/ {}));
        dataSourceDirectory->DataSources().back().SetAccount(intermediateAccount);
    } else {
        for (int index = 0; index < std::ssize(schemas); ++index) {
            dataSourceDirectory->DataSources().emplace_back(MakeUnversionedDataSource(
                GetIntermediatePath(index),
                schemas[index],
                /*columns*/ std::nullopt,
                /*omittedInaccessibleColumns*/ {},
                /*columnRenameDescriptors*/ {}));
            dataSourceDirectory->DataSources().back().SetAccount(intermediateAccount);
        }
    }

    return dataSourceDirectory;
}

TDataSink BuildIntermediateDataSink(const TString& intermediateAccount)
{
    TDataSink dataSink;
    dataSink.SetPath(GetIntermediatePath(0));
    dataSink.SetAccount(intermediateAccount);
    return dataSink;
}

TDataSinkDirectoryPtr BuildIntermediateDataSinkDirectory(const TString& intermediateAccount)
{
    auto dataSinkDirectory = New<TDataSinkDirectory>();
    dataSinkDirectory->DataSinks().emplace_back(BuildIntermediateDataSink(intermediateAccount));
    return dataSinkDirectory;
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

std::vector<TPartitionKey> BuildPartitionKeysBySamples(
    const std::vector<TSample>& samples,
    int partitionCount,
    const NTableClient::TComparator& comparator,
    const TRowBufferPtr& rowBuffer,
    const TLogger& logger)
{
    const auto& Logger = logger;

    YT_LOG_INFO("Building partition keys by samples (SampleCount: %v, PartitionCount: %v, Comparator: %v)", samples.size(), partitionCount, comparator);

    YT_VERIFY(partitionCount > 0);

    struct TComparableSample
    {
        TKeyBound KeyBound;

        bool Incomplete;

        i64 Weight;
    };

    std::vector<TComparableSample> comparableSamples;
    comparableSamples.reserve(samples.size());
    for (const auto& sample : samples) {
        comparableSamples.emplace_back(TComparableSample{
            .KeyBound = TKeyBound::FromRow(sample.Key, /*isInclusive*/true, /*isUpper*/false),
            .Incomplete = sample.Incomplete,
            .Weight = sample.Weight
        });
    }

    std::sort(comparableSamples.begin(), comparableSamples.end(), [&] (const auto& lhs, const auto& rhs) {
        auto result = comparator.CompareKeyBounds(lhs.KeyBound, rhs.KeyBound);
        if (result == 0) {
            return lhs.Incomplete < rhs.Incomplete;
        } else {
            return result < 0;
        }
    });

    std::vector<TPartitionKey> partitionKeys;

    i64 totalSamplesWeight = 0;
    for (const auto& sample : comparableSamples) {
        totalSamplesWeight += sample.Weight;
    }

    // Select samples evenly wrt weights.
    std::vector<const TComparableSample*> selectedSamples;
    selectedSamples.reserve(partitionCount - 1);

    double weightPerPartition = static_cast<double>(totalSamplesWeight) / partitionCount;
    i64 processedWeight = 0;
    for (const auto& sample : comparableSamples) {
        processedWeight += sample.Weight;
        if (processedWeight / weightPerPartition > selectedSamples.size() + 1) {
            selectedSamples.push_back(&sample);
        }
        if (std::ssize(selectedSamples) == partitionCount - 1) {
            // We need exactly partitionCount - 1 partition keys.
            break;
        }
    }

    // NB: Samples fetcher and controller use different row buffers.
    auto cloneKeyBound = [&] (const TKeyBound& keyBound) {
        auto capturedRow = rowBuffer->CaptureRow(keyBound.Prefix);
        return TKeyBound::FromRow(std::move(capturedRow), keyBound.IsInclusive, keyBound.IsUpper);
    };


    int sampleIndex = 0;
    while (sampleIndex < std::ssize(selectedSamples)) {
        auto lastLowerBound = TKeyBound::MakeUniversal(/*isUpper*/ false);
        if (!partitionKeys.empty()) {
            lastLowerBound = partitionKeys.back().LowerBound;
        }

        auto* sample = selectedSamples[sampleIndex];
        // Check for same keys.
        if (comparator.CompareKeyBounds(sample->KeyBound, lastLowerBound) != 0) {
            partitionKeys.emplace_back(cloneKeyBound(sample->KeyBound));
            ++sampleIndex;
        } else if (sampleIndex < std::ssize(selectedSamples)) {
            // Skip same keys.
            int skippedCount = 0;
            while (sampleIndex < std::ssize(selectedSamples) &&
                comparator.CompareKeyBounds(selectedSamples[sampleIndex]->KeyBound, lastLowerBound) == 0)
            {
                ++sampleIndex;
                ++skippedCount;
            }

            auto* lastManiacSample = selectedSamples[sampleIndex - 1];

            if (lastManiacSample->Incomplete) {
                // If sample keys are incomplete, we cannot use UnorderedMerge,
                // because full keys may be different.
                // Suppose we sort table by [a, b] and sampler returned trimmed keys [1], [1], [1] and [2].
                // Partitions [1, 1] and (1, 2] will be created, however first partition is not maniac since
                // might contain keys [1, "a"] and [1, "b"].

                if (sampleIndex >= std::ssize(selectedSamples)) {
                    break;
                }
                partitionKeys.emplace_back(cloneKeyBound(selectedSamples[sampleIndex]->KeyBound));
                ++sampleIndex;
            } else {
                partitionKeys.back().Maniac = true;
                YT_VERIFY(skippedCount >= 1);

                auto keyBound = cloneKeyBound(sample->KeyBound);
                YT_VERIFY(keyBound.IsInclusive);
                keyBound.IsInclusive = false;

                partitionKeys.emplace_back(keyBound);
            }
        }
    }

    return partitionKeys;
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TPartitionTreeSkeleton> BuildPartitionTreeSkeleton(int partitionCount, int maxPartitionFactor)
{
    YT_VERIFY(partitionCount > 0);
    YT_VERIFY(maxPartitionFactor > 0);

    maxPartitionFactor = std::max(maxPartitionFactor, 2);
    maxPartitionFactor = std::min(maxPartitionFactor, partitionCount);

    int partitionTreeDepth = 0;
    i64 maxPartitionFactorPower = 1;
    while (maxPartitionFactorPower < partitionCount) {
        ++partitionTreeDepth;
        maxPartitionFactorPower *= maxPartitionFactor;
    }

    partitionTreeDepth = std::max(partitionTreeDepth, 1);

    auto buildPartitionTreeSkeleton = [&] (int partitionCount, int depth, auto buildPartitionTreeSkeleton) {
        YT_VERIFY(partitionCount > 0);

        if (partitionCount == 1 && depth == 0) {
            return std::make_unique<TPartitionTreeSkeleton>();
        }

        auto partitionTreeSkeleton = std::make_unique<TPartitionTreeSkeleton>();

        int subtreeCount = std::min(maxPartitionFactor, partitionCount);
        int subtreeSize = partitionCount / subtreeCount;
        int largeSubtreeCount = partitionCount % subtreeCount;

        for (int subtreeIndex = 0; subtreeIndex < subtreeCount; ++subtreeIndex) {
            int currentSubtreeSize = subtreeSize;
            if (subtreeIndex < largeSubtreeCount) {
                ++currentSubtreeSize;
            }

            partitionTreeSkeleton->Children.emplace_back(buildPartitionTreeSkeleton(currentSubtreeSize, depth - 1, buildPartitionTreeSkeleton));
        }
        return partitionTreeSkeleton;
    };
    return buildPartitionTreeSkeleton(partitionCount, partitionTreeDepth, buildPartitionTreeSkeleton);
}

////////////////////////////////////////////////////////////////////////////////


TDiskQuota CreateDiskQuota(
    const TDiskRequestConfigPtr& diskRequestConfig,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
{
    if (!diskRequestConfig->MediumName) {
        return CreateDiskQuotaWithoutMedium(diskRequestConfig->DiskSpace);
    }
    // Enrich diskRequestConfig with MediumIndex.
    if (!diskRequestConfig->MediumIndex) {
        auto* mediumDescriptor = mediumDirectory->FindByName(*diskRequestConfig->MediumName);
        if (!mediumDescriptor) {
            THROW_ERROR_EXCEPTION("Unknown medium %Qv", *diskRequestConfig->MediumName);
        }
        diskRequestConfig->MediumIndex = mediumDescriptor->Index;
    }
    return NScheduler::CreateDiskQuota(*diskRequestConfig->MediumIndex, diskRequestConfig->DiskSpace);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
