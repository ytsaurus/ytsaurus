#include "helpers.h"

#include <yt/yt/server/controller_agent/config.h>
#include <yt/yt/server/controller_agent/controllers/helpers.h>
#include <yt/yt/server/controller_agent/operation_controller.h>

#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <yt/yt/ytlib/scheduler/config.h>
#include <yt/yt/ytlib/scheduler/job_resources_with_quota.h>

#include <yt/yt/library/query/engine_api/expression_evaluator.h>

#include <yt/yt/client/table_client/row_buffer.h>

// #include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/core/yson/protobuf_helpers.h>

#include <algorithm>

namespace NYT::NControllerAgent {

using namespace NChunkClient;
using namespace NLogging;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NScheduler;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

TString TrimCommandForBriefSpec(const std::string& command)
{
    const int MaxBriefSpecCommandLength = 256;
    // TODO(babenko): migrate to std::string
    TString commandT(command);
    return
        commandT.length() <= MaxBriefSpecCommandLength
        ? commandT
        : commandT.substr(0, MaxBriefSpecCommandLength) + "...";
}

////////////////////////////////////////////////////////////////////////////////

INodePtr UpdateSpec(INodePtr templateSpec, INodePtr originalSpec)
{
    if (!templateSpec) {
        return originalSpec;
    }
    return PatchNode(templateSpec, originalSpec);
}

////////////////////////////////////////////////////////////////////////////////

TUserFile::TUserFile(
    TRichYPath path,
    std::optional<TTransactionId> transactionId,
    bool layer)
    : TUserObject(std::move(path), transactionId)
    , Layer(layer)
{ }

void TUserFile::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TUserObject>();

    PHOENIX_REGISTER_FIELD(1, Attributes,
        .template Serializer<TAttributeDictionarySerializer>());
    PHOENIX_REGISTER_FIELD(2, FileName);
    PHOENIX_REGISTER_FIELD(3, ChunkSpecs);
    PHOENIX_REGISTER_FIELD(4, Type);
    PHOENIX_REGISTER_FIELD(5, Executable);
    PHOENIX_REGISTER_FIELD(6, Format);
    PHOENIX_REGISTER_FIELD(7, Schema,
        .template Serializer<TNonNullableIntrusivePtrSerializer<>>());
    PHOENIX_REGISTER_FIELD(8, Dynamic);
    PHOENIX_REGISTER_FIELD(9, Layer);
    PHOENIX_REGISTER_FIELD(10, Filesystem);
    PHOENIX_REGISTER_FIELD(11, AccessMethod);
    PHOENIX_REGISTER_FIELD(12, GpuCheck,
        .SinceVersion(ESnapshotVersion::PrepareGpuCheckFSDuration));
    PHOENIX_REGISTER_FIELD(13, RlsReadSpec,
        .SinceVersion(ESnapshotVersion::FileRlsReadSpec));
}

PHOENIX_DEFINE_TYPE(TUserFile);

////////////////////////////////////////////////////////////////////////////////

void BuildFileSpec(
    NProto::TFileDescriptor* descriptor,
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
        dataSource->SetObjectId(file.ObjectId);
        dataSource->SetAccount(file.Account);
        dataSource->SetRlsReadSpec(file.RlsReadSpec);

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
        dataSource->SetObjectId(file.ObjectId);
        dataSource->SetAccount(file.Account);
        dataSource->SetRlsReadSpec(file.RlsReadSpec);

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
                descriptor->set_format(ToProto(file.Format));
                break;
            default:
                YT_ABORT();
        }
    }

    if (file.Layer) {
        if (file.AccessMethod) {
            descriptor->set_access_method(ToProto(*file.AccessMethod));
        }

        if (file.Filesystem) {
            descriptor->set_filesystem(ToProto(*file.Filesystem));
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void BuildFileSpecs(
    NProto::TUserJobSpec* jobSpec,
    const std::vector<TUserFile>& files,
    const TUserJobSpecPtr& config,
    bool enableBypassArtifactCache)
{
    for (const auto& file : files) {
        NProto::TFileDescriptor* descriptor;
        if (file.GpuCheck) {
            descriptor = jobSpec->add_gpu_check_volume_layers();
        } else if (file.Layer) {
            continue;
        } else {
            descriptor = jobSpec->add_files();
        }

        BuildFileSpec(descriptor, file, config->CopyFiles, enableBypassArtifactCache);
    }
}

////////////////////////////////////////////////////////////////////////////////

TString GetIntermediatePath(int streamIndex)
{
    return Format("<intermediate-%v>", streamIndex);
}

TDataSourceDirectoryPtr BuildIntermediateDataSourceDirectory(
    const std::string& intermediateAccount,
    const std::vector<TTableSchemaPtr>& schemas)
{
    auto dataSourceDirectory = New<TDataSourceDirectory>();
    if (schemas.empty()) {
        dataSourceDirectory->DataSources().emplace_back(MakeUnversionedDataSource(
            GetIntermediatePath(0),
            New<TTableSchema>(),
            /*columns*/ std::nullopt,
            /*omittedInaccessibleColumns*/ {},
            /*columnRenameDescriptors*/ {}));
        dataSourceDirectory->DataSources().back()->SetAccount(intermediateAccount);
    } else {
        for (int index = 0; index < std::ssize(schemas); ++index) {
            dataSourceDirectory->DataSources().emplace_back(MakeUnversionedDataSource(
                GetIntermediatePath(index),
                schemas[index],
                /*columns*/ std::nullopt,
                /*omittedInaccessibleColumns*/ {},
                /*columnRenameDescriptors*/ {}));
            dataSourceDirectory->DataSources().back()->SetAccount(intermediateAccount);
        }
    }

    return dataSourceDirectory;
}

TDataSink BuildIntermediateDataSink(const std::string& intermediateAccount)
{
    TDataSink dataSink;
    dataSink.SetPath(GetIntermediatePath(0));
    dataSink.SetAccount(intermediateAccount);
    return dataSink;
}

TDataSinkDirectoryPtr BuildIntermediateDataSinkDirectory(const std::string& intermediateAccount)
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

std::vector<TPartitionKey> BuildPartitionKeysFromSamples(
    const std::vector<TSample>& samples,
    const TTableSchemaPtr& sampleSchema,
    const TTableSchemaPtr& uploadSchema,
    const IExpressionEvaluatorCachePtr& evaluatorCache,
    int partitionCount,
    const TRowBufferPtr& rowBuffer,
    const TLogger& logger)
{
    YT_VERIFY(partitionCount > 0);
    YT_VERIFY(uploadSchema->IsSorted());

    if (partitionCount == 1) {
        return {};
    }

    const auto& Logger = logger;

    auto comparator = uploadSchema->ToComparator();

    struct TSampleBufferTag
    { };

    auto sampleRowBuffer = New<TRowBuffer>(TSampleBufferTag());

    YT_LOG_INFO("Building partition keys by samples (SampleCount: %v, PartitionCount: %v, Comparator: %v)", samples.size(), partitionCount, comparator);

    struct TComparableSample
    {
        TKeyBound KeyBound;

        bool Incomplete;

        i64 Weight;
    };

    std::vector<std::function<TUnversionedValue(TUnversionedRow)>> columnEvaluators;
    columnEvaluators.reserve(uploadSchema->GetKeyColumnCount());

    for (int keyIndex = 0; keyIndex < uploadSchema->GetKeyColumnCount(); ++keyIndex) {
        const auto& column = uploadSchema->Columns()[keyIndex];

        if (const auto& expression = column.Expression()) {
            auto parsedSource = ParseSource(*expression, EParseMode::Expression);

            columnEvaluators.push_back([
                evaluator = evaluatorCache->Find(*parsedSource, sampleSchema),
                sampleRowBuffer
            ] (TUnversionedRow sampleRow) {
                return evaluator->Evaluate(sampleRow, sampleRowBuffer);
            });
        } else {
            int sampleColumn = sampleSchema->GetColumnIndex(column.Name());

            columnEvaluators.push_back([sampleColumn] (TUnversionedRow sampleRow) {
                return sampleRow[sampleColumn];
            });
        }
    }

    std::vector<TComparableSample> comparableSamples;
    comparableSamples.reserve(samples.size());
    for (const auto& sample : samples) {
        auto key = sampleRowBuffer->AllocateUnversioned(uploadSchema->GetKeyColumnCount());
        for (int index = 0; index < uploadSchema->GetKeyColumnCount(); ++index) {
            key[index] = columnEvaluators[index](sample.Key);
        }

        comparableSamples.emplace_back(TComparableSample{
            .KeyBound = TKeyBound::FromRow(key, /*isInclusive*/ true, /*isUpper*/ false),
            .Incomplete = sample.Incomplete,
            .Weight = sample.Weight,
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

        const auto* sample = selectedSamples[sampleIndex];
        // Check for same keys.
        if (comparator.CompareKeyBounds(sample->KeyBound, lastLowerBound) != 0) {
            partitionKeys.emplace_back(cloneKeyBound(sample->KeyBound));
            ++sampleIndex;
        } else {
            // Skip same keys.
            int skippedCount = 0;
            while (sampleIndex < std::ssize(selectedSamples) &&
                comparator.CompareKeyBounds(selectedSamples[sampleIndex]->KeyBound, lastLowerBound) == 0)
            {
                ++sampleIndex;
                ++skippedCount;
            }

            const auto* lastManiacSample = selectedSamples[sampleIndex - 1];

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

TPartitionTreeSkeleton BuildPartitionTreeSkeleton(int partitionCount, int maxPartitionFactor)
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
            return std::make_unique<TPartitionTreeSkeletonNode>();
        }

        auto partitionTreeSkeleton = std::make_unique<TPartitionTreeSkeletonNode>();

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
    return {
        .Root = buildPartitionTreeSkeleton(partitionCount, partitionTreeDepth, buildPartitionTreeSkeleton),
        .Depth = partitionTreeDepth,
    };
}

////////////////////////////////////////////////////////////////////////////////

TDiskQuota CreateDiskQuota(
    const TDiskRequestConfigPtr& diskRequestConfig,
    const TMediumDirectoryPtr& mediumDirectory)
{
    if (!diskRequestConfig->MediumName) {
        return CreateDiskQuotaWithoutMedium(diskRequestConfig->DiskSpace);
    }
    // Enrich diskRequestConfig with MediumIndex.
    if (!diskRequestConfig->MediumIndex) {
        auto mediumDescriptor = mediumDirectory->FindByName(*diskRequestConfig->MediumName);
        if (!mediumDescriptor) {
            THROW_ERROR_EXCEPTION("Unknown medium %Qv", *diskRequestConfig->MediumName);
        }
        diskRequestConfig->MediumIndex = mediumDescriptor->GetIndex();
    }
    return NScheduler::CreateDiskQuota(*diskRequestConfig->MediumIndex, diskRequestConfig->DiskSpace);
}

////////////////////////////////////////////////////////////////////////////////

PHOENIX_DEFINE_TEMPLATE_TYPE(TAvgSummary, int);

////////////////////////////////////////////////////////////////////////////////

void EnrichLayers(
    const TControllerAgentConfigPtr& config,
    const TOperationSpecBasePtr& operationSpec,
    const IOperationControllerHostPtr& host,
    TNonNullPtr<TUserJobSpec> spec)
{
    auto makeLayersFromRichYPaths = [] (const std::vector<NYPath::TRichYPath>& paths) {
        std::vector<TLayerPtr> layers;
        layers.reserve(paths.size());
        for (const auto& path : paths) {
            auto newLayer = New<TLayer>();
            newLayer->Path = path;
            layers.push_back(std::move(newLayer));
        }
        return layers;
    };
    auto enrichRootVolumeLayers = [&] (TVolumePtr& rootVolume) {
        if (!config->TestingOptions->RootfsTestLayers.empty()) {
            rootVolume->Layers = makeLayersFromRichYPaths(config->TestingOptions->RootfsTestLayers);
            return;
        }

        if (spec->DockerImage) {
            NYT::NControllerAgent::NControllers::TDockerImageSpec dockerImage(*spec->DockerImage, config->DockerRegistry);

            // External docker images are not compatible with any additional layers.
            if (!dockerImage.IsInternal || !config->DockerRegistry->TranslateInternalImagesIntoLayers) {
                return rootVolume->Layers.clear();
            }

            // Resolve internal docker image into base layers.
            auto layersFromDocker = makeLayersFromRichYPaths(GetLayerPathsFromDockerImage(host->GetClient(), dockerImage));
            rootVolume->Layers.insert(rootVolume->Layers.end(), layersFromDocker.begin(), layersFromDocker.end());
        }
        if (rootVolume->Layers.empty() && operationSpec->DefaultBaseLayerPath) {
            auto newLayer = New<TLayer>();
            // TODO(babenko): migrate to std::string
            newLayer->Path = TString(*operationSpec->DefaultBaseLayerPath);
            rootVolume->Layers.push_back(std::move(newLayer));
        }
        if (config->DefaultLayerPath && rootVolume->Layers.empty()) {
            // If no layers were specified, we insert the default one.
            auto newLayer = New<TLayer>();
            newLayer->Path = *config->DefaultLayerPath;
            rootVolume->Layers.push_back(std::move(newLayer));
        }

        if (config->CudaToolkitLayerDirectoryPath &&
            !rootVolume->Layers.empty() &&
            spec->CudaToolkitVersion &&
            spec->EnableGpuLayers)
        {
            // If cuda toolkit is requested, add the layer as the topmost user layer.
            auto newLayer = New<TLayer>();
            newLayer->Path = *config->CudaToolkitLayerDirectoryPath + "/" + *spec->CudaToolkitVersion;
            rootVolume->Layers.insert(rootVolume->Layers.begin(), std::move(newLayer));
        }

        if (spec->Profilers) {
            for (const auto& profilerSpec : *spec->Profilers) {
                auto cudaProfilerLayerPath = operationSpec->CudaProfilerLayerPath
                    ? operationSpec->CudaProfilerLayerPath
                    : config->CudaProfilerLayerPath;

                if (cudaProfilerLayerPath && profilerSpec->Type == EProfilerType::Cuda) {
                    auto newLayer = New<TLayer>();
                    // TODO(babenko): migrate to std::string
                    newLayer->Path = TString(*cudaProfilerLayerPath);
                    rootVolume->Layers.insert(rootVolume->Layers.begin(), std::move(newLayer));
                    break;
                }
            }
        }
        if (!rootVolume->Layers.empty()) {
            auto systemLayerPath = spec->SystemLayerPath
                ? spec->SystemLayerPath
                : config->SystemLayerPath;
            if (systemLayerPath) {
                // This must be the top layer, so insert in the beginning.
                auto newLayer = New<TLayer>();
                // TODO(babenko): migrate to std::string
                newLayer->Path = TString(*systemLayerPath);
                rootVolume->Layers.insert(rootVolume->Layers.begin(), std::move(newLayer));
            }
        }
    };

    auto getRootVolumeId = [] (const std::vector<TVolumeMountPtr>& volumeMounts) {
        auto it = std::find_if(volumeMounts.begin(), volumeMounts.end(), [] (const TVolumeMountPtr& volumeMount) {
            return volumeMount->MountPath == "/";
        });
        YT_VERIFY(it != volumeMounts.end());
        return (*it)->VolumeId;
    };

    auto it = GetIteratorOrCrash(spec->Volumes, getRootVolumeId(spec->JobVolumeMounts));
    enrichRootVolumeLayers(it->second);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NControllerAgent::NProto::TVolume* volumeProto,
    const NScheduler::TVolume& volume,
    const THashMap<TStringBuf, const NControllerAgent::TUserFile*>& layerPathToUserFile)
{
    if (volume.DiskRequest) {
        if (auto nbdDiskRequest = volume.DiskRequest->TryGetConcrete<TNbdDiskRequest>()) {
            auto protoDiskRequest = volumeProto->mutable_nbd_disk_request();
            ToProto(protoDiskRequest, *nbdDiskRequest);
        } else if (auto localDiskRequest = volume.DiskRequest->TryGetConcrete<TLocalDiskRequest>()) {
            auto protoDiskRequest = volumeProto->mutable_local_disk_request();
            ToProto(protoDiskRequest, *localDiskRequest);
        } else if (auto tmpfsDiskRequest = volume.DiskRequest->TryGetConcrete<TTmpfsStorageRequest>()) {
            ToProto(volumeProto->mutable_tmpfs_storage_request(), *tmpfsDiskRequest);
        } else {
            YT_ABORT();
        }
    }

    volumeProto->set_allow_reusing(volume.AllowReusing);
    for (const auto& layer : volume.Layers) {
        auto* file = GetOrCrash(layerPathToUserFile, layer->Path.GetPath());

        auto* descriptor = volumeProto->add_layers();
        BuildFileSpec(descriptor, *file, /* copyFiles */ false, /* enableBypassArtifactCache*/ false);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
