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

#include <yt/yt/core/misc/absolute_normalized_path.h>

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

using namespace NTableClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

std::string TrimCommandForBriefSpec(const std::string& command)
{
    const int MaxBriefSpecCommandLength = 256;
    return
        std::ssize(command) <= MaxBriefSpecCommandLength
        ? command
        : command.substr(0, MaxBriefSpecCommandLength) + "...";
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

NYPath::TYPath GetIntermediatePath(int streamIndex)
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

    auto enrichRootVolumeLayers = [&] (TVolumePtr& rootVolume, bool isSidecar) {
        if (!config->TestingOptions->RootfsTestLayers.empty()) {
            rootVolume->Layers = makeLayersFromRichYPaths(config->TestingOptions->RootfsTestLayers);
            return;
        }

        if (!isSidecar && spec->DockerImage) {
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
            newLayer->Path = *operationSpec->DefaultBaseLayerPath;
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

        if (!isSidecar && spec->Profilers) {
            for (const auto& profilerSpec : *spec->Profilers) {
                auto cudaProfilerLayerPath = operationSpec->CudaProfilerLayerPath
                    ? operationSpec->CudaProfilerLayerPath
                    : config->CudaProfilerLayerPath;

                if (cudaProfilerLayerPath && profilerSpec->Type == EProfilerType::Cuda) {
                    auto newLayer = New<TLayer>();
                    newLayer->Path = *cudaProfilerLayerPath;
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
                newLayer->Path = *systemLayerPath;
                rootVolume->Layers.insert(rootVolume->Layers.begin(), std::move(newLayer));
            }
        }
    };

    THashSet<std::string_view> volumesWasEnriched;
    auto getRootVolumeId = [] (const std::vector<TVolumeMountPtr>& volumeMounts) ->std::optional<std::string_view> {
        auto it = std::find_if(volumeMounts.begin(), volumeMounts.end(), [] (const TVolumeMountPtr& volumeMount) {
            return volumeMount->MountPath == "/";
        });
        if (it == volumeMounts.end()) {
            return std::nullopt;
        }
        return (*it)->VolumeId;
    };

    auto jobRootVolumeId = getRootVolumeId(spec->JobVolumeMounts);
    YT_VERIFY(jobRootVolumeId);
    auto& jobRootVolume = GetOrCrash(spec->Volumes, *jobRootVolumeId);
    enrichRootVolumeLayers(jobRootVolume, /*isSidecar*/ false);
    volumesWasEnriched.insert(*jobRootVolumeId);

    for (auto& [_, sidecar] : spec->Sidecars) {
        auto sidecarRootVolumeId = getRootVolumeId(sidecar->SidecarVolumeMounts);
        if (!sidecarRootVolumeId) {
            continue;
        }
        if (volumesWasEnriched.contains(*sidecarRootVolumeId)) {
            continue;
        }

        auto& sidecarRootVolume = GetOrCrash(spec->Volumes, *sidecarRootVolumeId);
        enrichRootVolumeLayers(sidecarRootVolume, /*isSidecar*/ true);
        volumesWasEnriched.insert(*sidecarRootVolumeId);
    }
}

void ValidateVolumeMountPaths(TNonNullPtr<TUserJobSpec>& providedUserSpec) {
    auto throwErrorIfPathIncorrect = [] (const std::filesystem::path& path) {
        try {
            TAbsoluteNormalizedPath tmp(path);
            if (tmp.Path().string() != path) {
                THROW_ERROR_EXCEPTION("Option \"mount_path\" must be normalized and absolute path")
                    << TErrorAttribute("mount_path", path);
            }
        } catch (...) {
            THROW_ERROR_EXCEPTION("Option \"mount_path\" must be absolute path")
                << TErrorAttribute("mount_path", path);
        }
    };

    for (const auto& volumeMount : providedUserSpec->JobVolumeMounts) {
        throwErrorIfPathIncorrect(volumeMount->MountPath);
    }

    for (const auto& [_, sidecar] : providedUserSpec->Sidecars) {
        for (const auto& volumeMount : sidecar->SidecarVolumeMounts) {
            throwErrorIfPathIncorrect(volumeMount->MountPath);
        }
    }
}

void ValidateSharedVolumes(TNonNullPtr<TUserJobSpec>& providedUserSpec) {
    struct TVolumeInfo {
        bool IsRootVolume = false;
        bool HasNestedVolumeMounts = false;
    };

    THashMap<std::string_view, TVolumeInfo> volumeInfos;
    auto validateVolumeMounts = [&] (const std::vector<NScheduler::TVolumeMountPtr>& volumeMounts) {
        for (i64 i = 0; i < std::ssize(volumeMounts); ++i) {
            TVolumeInfo currentVolumeInfo;
            TAbsoluteNormalizedPath currentPath(volumeMounts[i]->MountPath);
            for (i64 j = i + 1; j < std::ssize(volumeMounts); ++j) {
                if (currentPath.IsAncestorOf(volumeMounts[j]->MountPath)) {
                    currentVolumeInfo.HasNestedVolumeMounts = true;
                    break;
                }
            }
            if (volumeMounts[i]->MountPath == "/") {
                currentVolumeInfo.IsRootVolume = true;
            }

            auto it = volumeInfos.find(volumeMounts[i]->VolumeId);
            if (it != volumeInfos.end()) {
                if (currentVolumeInfo.HasNestedVolumeMounts || it->second.HasNestedVolumeMounts) {
                    THROW_ERROR_EXCEPTION(
                        "Shared volume %v cannot have nested volumes",
                        volumeMounts[i]->VolumeId);
                }

                if (currentVolumeInfo.IsRootVolume != it->second.IsRootVolume) {
                    THROW_ERROR_EXCEPTION(
                        "Root volume %v cannot be shared as non-root volumes",
                        volumeMounts[i]->VolumeId);
                }
            } else {
                volumeInfos[volumeMounts[i]->VolumeId] = currentVolumeInfo;
            }
        }
    };

    std::sort(providedUserSpec->JobVolumeMounts.begin(), providedUserSpec->JobVolumeMounts.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs->MountPath < rhs->MountPath;
    });
    validateVolumeMounts(providedUserSpec->JobVolumeMounts);

    for (auto& [_, sidecar] : providedUserSpec->Sidecars) {
        std::sort(sidecar->SidecarVolumeMounts.begin(), sidecar->SidecarVolumeMounts.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs->MountPath < rhs->MountPath;
        });
        validateVolumeMounts(sidecar->SidecarVolumeMounts);
    }
}

void ValidateProvidedVolumeMountsSpec(TNonNullPtr<TUserJobSpec>& providedUserSpec)
{
    ValidateVolumeMountPaths(providedUserSpec);
    ValidateSharedVolumes(providedUserSpec);
}

void ValidateAndEnrichVolumeSpec(TNonNullPtr<TUserJobSpec> spec)
{
    if (!spec->DeprecatedTmpfsVolumes.empty() && !spec->Volumes.empty()) {
        THROW_ERROR_EXCEPTION(
            "Option \"tmpfs_volumes\" cannot be specified simultaneously with \"volumes\"")
            << TErrorAttribute("tmpfs_volumes", spec->DeprecatedTmpfsVolumes)
            << TErrorAttribute("volumes", spec->Volumes);
    }

    if (spec->DiskSpaceLimit && !spec->Volumes.empty()) {
        THROW_ERROR_EXCEPTION(
            "Options \"disk_space_limit\" cannot be specified "
            "together with \"volumes\" which contains not only tmpfs volumes")
            << TErrorAttribute("disk_space_limit", spec->DiskSpaceLimit)
            << TErrorAttribute("inode_limit", spec->InodeLimit)
            << TErrorAttribute("volumes", spec->Volumes);
    }

    if (spec->DeprecatedDiskRequest && !spec->Volumes.empty()) {
        THROW_ERROR_EXCEPTION(
            "Option \"disk_request\" cannot be specified simultaneously with \"volumes\"")
            << TErrorAttribute("disk_request", spec->DeprecatedDiskRequest)
            << TErrorAttribute("volumes", spec->Volumes);
    }

    if (!spec->DeprecatedLayerPaths.empty() && !spec->Volumes.empty()) {
        THROW_ERROR_EXCEPTION(
            "Option \"layer_paths\" cannot be specified simultaneously with \"volumes\"")
            << TErrorAttribute("layer_paths", spec->DeprecatedDiskRequest)
            << TErrorAttribute("volumes", spec->Volumes);
    }

    auto forEachForVolumeMounts = [&] (const auto& f) {
        f(spec->JobVolumeMounts);
        for (const auto& [_, sidecar] : spec->Sidecars) {
            f(sidecar->SidecarVolumeMounts);
        }
    };

    {
        THashSet<std::string> requestedVolumeIds;

        auto addRequestedVolumeIdsFromVolumeMounts = [&] (const std::vector<TVolumeMountPtr>& volumeMounts) {
            for (const auto& volumeMount : volumeMounts) {
                requestedVolumeIds.insert(volumeMount->VolumeId);
                if (!spec->Volumes.contains(volumeMount->VolumeId)) {
                    THROW_ERROR_EXCEPTION("Volume was requested but not described")
                        << TErrorAttribute("volume_id", volumeMount->VolumeId);
                }
            }
        };

        forEachForVolumeMounts(addRequestedVolumeIdsFromVolumeMounts);

        for (const auto& [id, volume] : spec->Volumes) {
            if (!requestedVolumeIds.contains(id)) {
                THROW_ERROR_EXCEPTION("Volume was described but not used")
                    << TErrorAttribute("volume_id", id);
            }
        }
    }

    auto makeNewNameForVolume = [index = 0, &spec] () mutable {
        while (spec->Volumes.contains(ToString(index))) {
            ++index;
        }
        return ToString(index++);
    };

    for (auto& [_, sidecar] : spec->Sidecars) {
        std::optional<std::string_view> sidecarRootVolume;
        if (sidecar->DockerImage && !sidecar->SidecarVolumeMounts.empty()) {
            THROW_ERROR_EXCEPTION("Using both volumes and a Docker image in a sidecar is not allowed")
                << TErrorAttribute("sidecar", sidecar);
        }

        if (sidecar->DockerImage) {
            continue;
        }

        for (const auto& volumeMount : sidecar->SidecarVolumeMounts) {
            if (volumeMount->MountPath == "/") {
                sidecarRootVolume = volumeMount->VolumeId;
                auto& volume = spec->Volumes[volumeMount->VolumeId];
                if (!volume->DiskRequest) {
                    THROW_ERROR_EXCEPTION("Sidecar root volume must have \"disk_request\"")
                        << TErrorAttribute("volume_id", volumeMount->VolumeId)
                        << TErrorAttribute("volume", volume);
                }
                break;
            }
        }
        if (!sidecarRootVolume) {
            THROW_ERROR_EXCEPTION("Options \"SidecarVolumeMounts\" must have root volume")
                << TErrorAttribute("sidecar_volume_mounts", sidecar->SidecarVolumeMounts);
        }
    }

    TVolumePtr newRootVolume;
    TVolumeMountPtr newVolumeMount;
    auto newNameForNewVolume = makeNewNameForVolume();

    bool hasRootFSInJobVolumeMounts = [&] () {
        auto it = std::find_if(spec->JobVolumeMounts.begin(), spec->JobVolumeMounts.end(), [] (const auto& volumeMount) {
            return volumeMount->MountPath == "/";
        });
        return it != spec->JobVolumeMounts.end();
    }();
    if (!hasRootFSInJobVolumeMounts) {
        newRootVolume = New<TVolume>();

        newVolumeMount = New<TVolumeMount>();
        newVolumeMount->MountPath = "/";
        newVolumeMount->VolumeId = newNameForNewVolume;
        newVolumeMount->ReadOnly = false;
    }

    if (spec->DeprecatedDiskRequest) {
        if (spec->DeprecatedDiskRequest->NbdDisk) {
            newRootVolume->DiskRequest = TStorageRequestConfig(NExecNode::EVolumeType::Nbd);
            const auto& diskRequest = newRootVolume->DiskRequest->GetConcrete<NExecNode::EVolumeType::Nbd>();
            *diskRequest = spec->DeprecatedDiskRequest;
        } else {
            newRootVolume->DiskRequest = TStorageRequestConfig(NExecNode::EVolumeType::LocalDisk);
            const auto& diskRequest = newRootVolume->DiskRequest->GetConcrete<NExecNode::EVolumeType::LocalDisk>();
            *diskRequest = spec->DeprecatedDiskRequest;
        }
    }

    for (const auto& volumeFromOldSpec : spec->DeprecatedTmpfsVolumes) {
        auto nameForNewVolume = makeNewNameForVolume();

        if (!NFS::IsPathRelativeAndInvolvesNoTraversal(volumeFromOldSpec->Path)) {
            THROW_ERROR_EXCEPTION(
                "Tmpfs path %v does not point inside the sandbox directory",
                volumeFromOldSpec->Path);
        }

        auto volumeMount = New<TVolumeMount>();
        volumeMount->MountPath = std::filesystem::path(std::string(volumeFromOldSpec->Path));
        volumeMount->VolumeId = nameForNewVolume;
        volumeMount->ReadOnly = false;
        spec->JobVolumeMounts.push_back(std::move(volumeMount));

        auto newVolume = New<TVolume>();
        newVolume->DiskRequest = TStorageRequestConfig(NExecNode::EVolumeType::Tmpfs);
        (*newVolume->DiskRequest)->DiskSpace = volumeFromOldSpec->Size;
        spec->Volumes[std::move(nameForNewVolume)] = std::move(newVolume);
    }

    i64 totalTmpfsSize = 0;
    for (const auto& [_, volume] : spec->Volumes) {
        if (!IsDiskRequestTmpfs(volume->DiskRequest)) {
            continue;
        }

        totalTmpfsSize += (*volume->DiskRequest)->DiskSpace;
    }

    // Memory reserve should be greater than or equal to tmpfs_size (see YT-5518 for more details).
    if (totalTmpfsSize > spec->MemoryLimit) {
        THROW_ERROR_EXCEPTION("Total size of tmpfs volumes must be less than or equal to memory limit")
            << TErrorAttribute("tmpfs_size", totalTmpfsSize)
            << TErrorAttribute("memory_limit", spec->MemoryLimit);
    }

    if (spec->MemoryReserveFactor &&
        (*spec->MemoryReserveFactor == 1.0 || !spec->IgnoreMemoryReserveFactorLessThanOne))
    {
        spec->UserJobMemoryDigestLowerBound = spec->UserJobMemoryDigestDefaultValue = *spec->MemoryReserveFactor;
    }

    auto memoryDigestLowerLimit = static_cast<double>(totalTmpfsSize) / spec->MemoryLimit;
    spec->UserJobMemoryDigestDefaultValue = std::min(
        1.0,
        std::max(spec->UserJobMemoryDigestDefaultValue, memoryDigestLowerLimit));
    spec->UserJobMemoryDigestLowerBound = std::min(
        1.0,
        std::max(spec->UserJobMemoryDigestLowerBound, memoryDigestLowerLimit));
    spec->UserJobMemoryDigestDefaultValue = std::max(spec->UserJobMemoryDigestLowerBound, spec->UserJobMemoryDigestDefaultValue);

    if (!spec->DiskSpaceLimit && spec->InodeLimit) {
        THROW_ERROR_EXCEPTION("Option \"inode_limit\" can be specified only with \"disk_space_limit\"");
    }

    if (spec->DiskSpaceLimit) {
        newRootVolume->DiskRequest = TStorageRequestConfig(NExecNode::EVolumeType::LocalDisk);
        const auto& diskRequest = newRootVolume->DiskRequest->GetConcrete<NExecNode::EVolumeType::LocalDisk>();

        diskRequest->DiskSpace = *spec->DiskSpaceLimit;
        diskRequest->InodeCount = spec->InodeLimit;

        spec->DiskSpaceLimit = std::nullopt;
        spec->InodeLimit = std::nullopt;
    }

    auto copyLayersToVolume = [] (TVolumePtr volume, const std::vector<NYPath::TRichYPath>& layerPaths) {
        volume->Layers.reserve(layerPaths.size());
        for (const auto& layerPath : layerPaths) {
            auto newLayer = New<TLayer>();
            newLayer->Path = layerPath;
            volume->Layers.push_back(std::move(newLayer));
        }
    };

    if (!spec->DeprecatedLayerPaths.empty()) {
        copyLayersToVolume(newRootVolume, spec->DeprecatedLayerPaths);
    }

    if (newRootVolume) {
        spec->Volumes[newNameForNewVolume] = std::move(newRootVolume);
        spec->JobVolumeMounts.push_back(std::move(newVolumeMount));
    }

    {
        THashSet<std::string_view> allUniqueVolumeMountPaths;
        for (const auto& volumeMount : spec->JobVolumeMounts) {
            if (!allUniqueVolumeMountPaths.insert(volumeMount->MountPath.native()).second) {
                THROW_ERROR_EXCEPTION("Options \"job_volume_mounts\" must contains only unique mount path")
                    << TErrorAttribute("job_volume_mounts", spec->JobVolumeMounts)
                    << TErrorAttribute("volume_id", volumeMount->VolumeId);
            }
        }
    }
    for (const auto& [_, sidecar] : spec->Sidecars) {
        THashSet<std::string_view> allUniqueSidecarVolumeMountPaths;
        for (const auto& volumeMount : sidecar->SidecarVolumeMounts) {
            if (!allUniqueSidecarVolumeMountPaths.insert(volumeMount->MountPath.native()).second) {
                THROW_ERROR_EXCEPTION("Options \"sidecar_volume_mounts\" must contains only unique mount path")
                    << TErrorAttribute("sidecar_volume_mounts", sidecar->SidecarVolumeMounts)
                    << TErrorAttribute("volume_id", volumeMount->VolumeId);
            }
        }
    }

    THashSet<std::string> allVolumesMediums;
    bool hasNonRootNbdVolume = false;
    auto rootVolumeIds = [&] () -> THashSet<std::string_view> {
        THashSet<std::string_view> result;

        auto addRootVolumeIdByVolumeMount = [&] (const std::vector<TVolumeMountPtr>& volumeMounts) {
            for (const auto& volumeMount : volumeMounts) {
                if (volumeMount->MountPath == "/") {
                    result.insert(volumeMount->VolumeId);
                }
            }
        };

        forEachForVolumeMounts(addRootVolumeIdByVolumeMount);
        return result;
    }();

    for (const auto& [volumeId, volume] : spec->Volumes) {
        if (!volume->DiskRequest) {
            continue;
        }

        if (const auto& diskRequest = volume->DiskRequest->TryGetConcrete<NScheduler::TDiskRequestConfig>()) {
            if (diskRequest->MediumName) {
                allVolumesMediums.insert(*diskRequest->MediumName);
            }
        }

        if (rootVolumeIds.contains(volumeId)) {
            if (volume->DiskRequest->GetType() == NExecNode::EVolumeType::Tmpfs) {
                THROW_ERROR_EXCEPTION("Root tmpfs are not supported")
                    << TErrorAttribute("volumes", spec->Volumes);
            }
            continue;
        }

        if (auto volumeType = volume->DiskRequest->GetType(); volumeType == NExecNode::EVolumeType::Nbd) {
            hasNonRootNbdVolume = true;
        }
    }

    int tmpfsVolumeIndex = 0;
    for (const auto& jobVolumeMount : spec->JobVolumeMounts) {
        auto& volume = GetOrCrash(spec->Volumes, jobVolumeMount->VolumeId);
        if (!IsDiskRequestTmpfs(volume->DiskRequest)) {
            continue;
        }
        // COMPAT (krasovav)
        volume->DiskRequest->GetConcrete<TTmpfsStorageRequest>()->TmpfsIndex = tmpfsVolumeIndex++;
    }

    std::vector<std::string> sidecarNames;
    sidecarNames.reserve(spec->Sidecars.size());
    for (const auto& [name, _] : spec->Sidecars) {
        sidecarNames.push_back(name);
    }

    std::sort(sidecarNames.begin(), sidecarNames.end());
    for (const auto& sidecarName : sidecarNames) {
        const auto& sidecar = GetOrCrash(spec->Sidecars, sidecarName);
        for (const auto& volumeMount : sidecar->SidecarVolumeMounts) {
            auto& volume = GetOrCrash(spec->Volumes, volumeMount->VolumeId);
            if (!IsDiskRequestTmpfs(volume->DiskRequest) || volume->DiskRequest->GetConcrete<TTmpfsStorageRequest>()->TmpfsIndex) {
                continue;
            }
            // COMPAT (krasovav)
            volume->DiskRequest->GetConcrete<TTmpfsStorageRequest>()->TmpfsIndex = tmpfsVolumeIndex++;
        }
    }

    for (const auto& [volumeId, volume] : spec->Volumes) {
        if (rootVolumeIds.contains(volumeId)) {
            continue;
        }
        if (!volume->DiskRequest) {
            THROW_ERROR_EXCEPTION("Options \"volumes\" must contains disk_request for non-root volume")
                << TErrorAttribute("volume_id", volumeId);
        }
    }

    // TODO(krasovav): Delete after supporting multiple medium.
    if (allVolumesMediums.size() > 1) {
        THROW_ERROR_EXCEPTION("Disk requests with two or more different medium are not currently supported")
            << TErrorAttribute("volumes", spec->Volumes);
    }

    if (hasNonRootNbdVolume) {
        THROW_ERROR_EXCEPTION("Non-root NBD are not currently supported")
            << TErrorAttribute("volumes", spec->Volumes);
    }
}

void ValidateAndEnrichVolumeSpec(
    const TControllerAgentConfigPtr& config,
    const TOperationSpecBasePtr& operationSpec,
    const IOperationControllerHostPtr& host,
    TNonNullPtr<NScheduler::TUserJobSpec> spec,
    TNonNullPtr<NScheduler::TUserJobSpec> providedSpec)
{
    ValidateProvidedVolumeMountsSpec(providedSpec);
    ValidateAndEnrichVolumeSpec(spec);
    EnrichLayers(config, operationSpec, host, spec);
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
        BuildFileSpec(descriptor, *file, /*copyFiles*/ false, /*enableBypassArtifactCache*/ false);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
