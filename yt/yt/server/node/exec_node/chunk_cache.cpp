#include "chunk_cache.h"

#include "bootstrap.h"
#include "master_connector.h"
#include "private.h"

#include <yt/yt/server/node/data_node/artifact.h>
#include <yt/yt/server/node/data_node/blob_chunk.h>
#include <yt/yt/server/node/data_node/blob_reader_cache.h>
#include <yt/yt/server/node/data_node/chunk_store.h>
#include <yt/yt/server/node/data_node/config.h>
#include <yt/yt/server/node/data_node/location.h>
#include <yt/yt/server/node/data_node/master_connector.h>
#include <yt/yt/server/node/data_node/private.h>

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

#include <yt/yt/server/lib/io/chunk_file_reader.h>
#include <yt/yt/server/lib/io/chunk_file_writer.h>
#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_memory_manager.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>
#include <yt/yt/ytlib/chunk_client/block_fetcher.h>

#include <yt/yt/ytlib/file_client/file_chunk_reader.h>

#include <yt/yt/ytlib/hydra/peer_channel.h>

#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>

#include <yt/yt/client/api/config.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/formats/config.h>
#include <yt/yt/client/formats/format.h>

#include <yt/yt/client/chunk_client/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/client/misc/io_tags.h>

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>
#include <library/cpp/yt/memory/ref.h>

#include <util/generic/algorithm.h>

#include <util/random/random.h>

namespace NYT::NExecNode {

using namespace NYTree;
using namespace NYson;
using namespace NChunkClient;
using namespace NDataNode;
using namespace NIO;
using namespace NObjectClient;
using namespace NFileClient;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NClusterNode;
using namespace NRpc;
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NApi;
using namespace NFormats;
using namespace NLogging;
using namespace NTracing;

using NChunkClient::TDataSliceDescriptor;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ExecNodeLogger;
static const int TableArtifactBufferRowCount = 10000;

////////////////////////////////////////////////////////////////////////////////

class TSessionCounterGuard
{
public:
    explicit TSessionCounterGuard(TChunkLocationPtr location)
        : Location_(std::move(location))
    {
        Location_->UpdateSessionCount(ESessionType::User, +1);
    }

    TSessionCounterGuard(TSessionCounterGuard&& other) = default;

    ~TSessionCounterGuard()
    {
        if (Location_) {
            Location_->UpdateSessionCount(ESessionType::User, -1);
        }
    }

private:
    TChunkLocationPtr Location_;
};

////////////////////////////////////////////////////////////////////////////////

class TErrorInterceptingOutput
    : public IOutputStream
{
public:
    TErrorInterceptingOutput(TChunkLocationPtr location, IOutputStream* underlying)
        : Location_(std::move(location))
        , Underlying_(underlying)
    { }

private:
    const TChunkLocationPtr Location_;
    IOutputStream* const Underlying_;


    void DoWrite(const void* buf, size_t len) override
    {
        try {
            Underlying_->Write(buf, len);
        } catch (const std::exception& ex) {
            Location_->ScheduleDisable(ex);
        }
    }

    void DoWriteV(const TPart* parts, size_t count) override
    {
        try {
            Underlying_->Write(parts, count);
        } catch (const std::exception& ex) {
            Location_->ScheduleDisable(ex);
        }
    }

    void DoFlush() override
    {
        try {
            Underlying_->Flush();
        } catch (const std::exception& ex) {
            Location_->ScheduleDisable(ex);
        }
    }

    void DoFinish() override
    {
        try {
            Underlying_->Finish();
        } catch (const std::exception& ex) {
            Location_->ScheduleDisable(ex);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TErrorInterceptingChunkWriter
    : public IChunkWriter
{
public:
    TErrorInterceptingChunkWriter(TChunkLocationPtr location, IChunkWriterPtr underlying)
        : Location_(std::move(location))
        , Underlying_(std::move(underlying))
    { }

    TFuture<void> Open() override
    {
        return Check(Underlying_->Open());
    }

    TFuture<void> Cancel() override
    {
        return Underlying_->Cancel();
    }

    bool WriteBlock(const TWorkloadDescriptor& workloadDescriptor, const TBlock& block) override
    {
        return Underlying_->WriteBlock(workloadDescriptor, block);
    }

    bool WriteBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<TBlock>& blocks) override
    {
        return Underlying_->WriteBlocks(workloadDescriptor, blocks);
    }

    TFuture<void> GetReadyEvent() override
    {
        return Check(Underlying_->GetReadyEvent());
    }

    TFuture<void> Close(
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TDeferredChunkMetaPtr& chunkMeta) override
    {
        return Check(Underlying_->Close(workloadDescriptor, chunkMeta));
    }

    const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override
    {
        return Underlying_->GetChunkInfo();
    }

    TChunkReplicaWithLocationList GetWrittenChunkReplicas() const override
    {
        return Underlying_->GetWrittenChunkReplicas();
    }

    TChunkId GetChunkId() const override
    {
        return Underlying_->GetChunkId();
    }

    NErasure::ECodec GetErasureCodecId() const override
    {
        return Underlying_->GetErasureCodecId();
    }

    const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const override
    {
        return Underlying_->GetDataStatistics();
    }

    bool IsCloseDemanded() const override
    {
        return Underlying_->IsCloseDemanded();
    }

private:
    const TChunkLocationPtr Location_;
    const IChunkWriterPtr Underlying_;


    TFuture<void> Check(TFuture<void> result)
    {
        return result.Apply(BIND([location = Location_] (const TError& error) {
            if (!error.IsOK()) {
                location->ScheduleDisable(error);
            }
        }));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TArtifactMetaHeader
{
    ui64 Signature = ExpectedSignature;
    ui64 Version = ExpectedVersion;

    static constexpr ui64 ExpectedSignature = 0x313030484d415459ull; // YTAMH001
    static constexpr ui64 ExpectedVersion = 4;
};

constexpr ui64 TArtifactMetaHeader::ExpectedSignature;
constexpr ui64 TArtifactMetaHeader::ExpectedVersion;

struct TArtifactReaderMetaBufferTag { };

////////////////////////////////////////////////////////////////////////////////

class TChunkCache::TImpl
    : public TAsyncSlruCacheBase<TArtifactKey, TCachedBlobChunk>
{
public:
    DEFINE_BYREF_RO_PROPERTY(std::vector<TCacheLocationPtr>, Locations);

public:
    TImpl(TDataNodeConfigPtr config, IBootstrap* bootstrap)
        : TAsyncSlruCacheBase(
            TSlruCacheConfig::CreateWithCapacity(config->GetCacheCapacity()),
            ExecNodeProfiler.WithPrefix("/chunk_cache"))
        , Config_(config)
        , Bootstrap_(bootstrap)
        , ArtifactCacheReaderConfig_(New<TArtifactCacheReaderConfig>())
    { }

    void Initialize()
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        YT_LOG_INFO("Initializing chunk cache");

        Bootstrap_->GetDynamicConfigManager()->SubscribeConfigChanged(
            BIND(&TChunkCache::TImpl::OnDynamicConfigChanged, MakeWeak(this)));

        std::vector<TFuture<void>> futures;
        for (int index = 0; index < std::ssize(Config_->CacheLocations); ++index) {
            auto locationConfig = Config_->CacheLocations[index];

            auto location = New<TCacheLocation>(
                "cache" + ToString(index),
                locationConfig,
                Bootstrap_->GetDynamicConfigManager(),
                TChunkContext::Create(Bootstrap_),
                CreateChunkStoreHost(Bootstrap_),
                Bootstrap_->GetChunkCache());

            futures.push_back(
                BIND(&TImpl::InitializeLocation, MakeStrong(this))
                    .AsyncVia(location->GetAuxPoolInvoker())
                    .Run(location));

            Locations_.push_back(location);
        }

        WaitFor(AllSucceeded(futures))
            .ThrowOnError();

        if (!Locations_.empty()) {
            const auto& mediumName = Locations_.front()->GetMediumName();
            for (const auto& location : Locations_) {
                if (location->GetMediumName() != mediumName) {
                    THROW_ERROR_EXCEPTION(
                        "Locations %v and %v are configured with distinct media (%Qv != %Qv), "
                        "but multiple cache media on one host are not supported yet",
                        Locations_.front()->GetId(),
                        location->GetId(),
                        mediumName,
                        location->GetMediumName());

                }
            }
        }

        YT_LOG_INFO("Chunk cache initialized (ChunkCount: %v)",
            GetSize());

        RunBackgroundValidation();
    }

    bool IsEnabled() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        for (const auto& location : Locations_) {
            if (location->IsEnabled()) {
                return true;
            }
        }
        return false;
    }

    IChunkPtr FindChunk(TChunkId chunkId)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Find(TArtifactKey(chunkId));
    }

    std::vector<IChunkPtr> GetChunks()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto chunks = GetAll();
        return std::vector<IChunkPtr>(chunks.begin(), chunks.end());
    }

    TFuture<IChunkPtr> DownloadArtifact(
        const TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions,
        bool* fetchedFromCache = nullptr)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto traceContext = CreateTraceContextFromCurrent("ChunkCache");
        TTraceContextGuard guard(traceContext);
        auto baggage = traceContext->UnpackOrCreateBaggage();
        AddTagToBaggage(baggage, EAggregateIOTag::JobIoKind, "artifact_download");
        AddTagsFromDataSource(baggage, FromProto<NChunkClient::TDataSource>(key.data_source()));
        traceContext->PackBaggage(std::move(baggage));

        auto chunkReadOptions = MakeClientChunkReadOptions(
            artifactDownloadOptions,
            /*bypassArtifactCache*/ false);

        auto Logger = ExecNodeLogger.WithTag("Key: %v, ReadSessionId: %v",
            key,
            chunkReadOptions.ReadSessionId);

        auto cookie = BeginInsert(key);
        auto cookieValue = cookie.GetValue();

        if (cookie.IsActive()) {
            if (auto optionalDescriptor = ExtractRegisteredChunk(key)) {
                DoValidateArtifact(std::move(cookie), key, artifactDownloadOptions, chunkReadOptions, *optionalDescriptor, Logger);
            } else {
                DoDownloadArtifact(std::move(cookie), key, artifactDownloadOptions, chunkReadOptions, Logger);
            }
            if (fetchedFromCache) {
                *fetchedFromCache = false;
            }
        } else {
            YT_LOG_INFO("Artifact is either found in cache or is being downloaded");
            if (fetchedFromCache) {
                *fetchedFromCache = true;
            }
        }
        return cookieValue.As<IChunkPtr>();
    }

    std::function<void(IOutputStream*)> MakeArtifactDownloadProducer(
        const TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto chunkReadOptions = MakeClientChunkReadOptions(
            artifactDownloadOptions,
            /*bypassArtifactCache*/ true);

        decltype(&TImpl::MakeFileProducer) producerBuilder;
        switch (CheckedEnumCast<EDataSourceType>(key.data_source().type())) {
            case EDataSourceType::File:
                producerBuilder = &TImpl::MakeFileProducer;
                break;
            case EDataSourceType::UnversionedTable:
            case EDataSourceType::VersionedTable:
                producerBuilder = &TImpl::MakeTableProducer;
                break;
            default:
                YT_ABORT();
        }

        return (this->*producerBuilder)(
            key,
            artifactDownloadOptions.TrafficMeter,
            chunkReadOptions,
            // TODO(babenko): throttle preparation
            GetUnlimitedThrottler());
    }

    TFuture<void> RemoveChunksByLocation(const TCacheLocationPtr& location)
    {
        return BIND([=, this, this_ = MakeStrong(this)] () {
            auto chunks = GetAll();
            for (const auto& chunk : chunks) {
                if (chunk->GetLocation() == location) {
                    TryRemove(chunk->GetKey(), /*forbidResurrection*/ true);
                }
            }

            {
                auto guard = Guard(RegisteredChunkMapLock_);
                THashMap<TArtifactKey, TRegisteredChunkDescriptor> newRegisteredChunkMap;
                for (const auto& [artifactKey, chunkDescriptor] : RegisteredChunkMap_) {
                    if (chunkDescriptor.Location != location) {
                        newRegisteredChunkMap.emplace(artifactKey, chunkDescriptor);
                    }
                }

                RegisteredChunkMap_ = std::move(newRegisteredChunkMap);
            }
        })
            .AsyncVia(Bootstrap_->GetControlInvoker())
            .Run();
    }

private:
    const TDataNodeConfigPtr Config_;
    IBootstrap* const Bootstrap_;

    TAtomicIntrusivePtr<TArtifactCacheReaderConfig> ArtifactCacheReaderConfig_;

    //! Describes a registered but not yet validated chunk.
    struct TRegisteredChunkDescriptor
    {
        TCacheLocationPtr Location;
        TChunkDescriptor Descriptor;
    };

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, RegisteredChunkMapLock_);
    THashMap<TArtifactKey, TRegisteredChunkDescriptor> RegisteredChunkMap_;


    void InitializeLocation(const TCacheLocationPtr& location)
    {
        VERIFY_INVOKER_AFFINITY(location->GetAuxPoolInvoker());

        auto descriptors = location->Scan();

        location->InitializeIds();

        if (location->GetState() == ELocationState::Crashed) {
            return;
        }

        for (const auto& descriptor : descriptors) {
            RegisterChunk(location, descriptor);
        }

        location->Start();
    }

    void RunBackgroundValidation()
    {
        Bootstrap_->GetStorageHeavyInvoker()->Invoke(BIND([this_ = MakeStrong(this), this] () {
            // Delay start of background validation to populate chunk cache with useful artifacts.
            TDelayedExecutor::WaitForDuration(Config_->BackgroundArtifactValidationDelay);

            YT_LOG_INFO("Background artifacts validation started");

            while (true) {
                auto guard = Guard(RegisteredChunkMapLock_);
                if (RegisteredChunkMap_.empty()) {
                    YT_LOG_INFO("Background artifacts validation finished");
                    return;
                }

                auto artifactKey = RegisteredChunkMap_.begin()->first;
                guard.Release();

                TArtifactDownloadOptions options;
                auto errorOrChunk = WaitFor(DownloadArtifact(artifactKey, options));
                if (!errorOrChunk.IsOK()) {
                    YT_LOG_WARNING(errorOrChunk, "Background artifact validation failed (ArtifactKey: %v)", artifactKey);
                }

                TDelayedExecutor::WaitForDuration(TDuration::MilliSeconds(10));
            }}));
    }

    std::optional<TRegisteredChunkDescriptor> ExtractRegisteredChunk(const TArtifactKey& key)
    {
        auto guard = Guard(RegisteredChunkMapLock_);
        auto it = RegisteredChunkMap_.find(key);
        if (it == RegisteredChunkMap_.end()) {
            return std::nullopt;
        }
        auto descriptor = std::move(it->second);
        RegisteredChunkMap_.erase(it);
        return descriptor;
    }

    void DoDownloadArtifact(
        TInsertCookie cookie,
        const TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions,
        const TClientChunkReadOptions& chunkReadOptions,
        const NLogging::TLogger& Logger)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_INFO("Loading artifact into cache");

        auto cookieValue = cookie.GetValue();
        auto canPrepareSingleChunk = CanPrepareSingleChunk(key);
        auto chunkId = GetOrCreateArtifactId(key, canPrepareSingleChunk);

        auto [location, lockedChunkGuard] = AcquireNewChunkLocation(chunkId);
        if (!location) {
            auto error = TError("Cannot find a suitable location for artifact chunk");
            cookie.Cancel(error);
            YT_LOG_ERROR(error);
            return;
        }

        decltype(&TImpl::DownloadChunk) downloader;
        if (canPrepareSingleChunk) {
            downloader = &TImpl::DownloadChunk;
        } else {
            switch (CheckedEnumCast<EDataSourceType>(key.data_source().type())) {
                case EDataSourceType::File:
                    downloader = &TImpl::DownloadFile;
                    break;
                case EDataSourceType::UnversionedTable:
                case EDataSourceType::VersionedTable:
                    downloader = &TImpl::DownloadTable;
                    break;
                default:
                    YT_ABORT();
            }
        }

        TSessionCounterGuard sessionCounterGuard(location);

        auto invoker = CreateSerializedInvoker(location->GetAuxPoolInvoker());
        invoker->Invoke(BIND(
            downloader,
            MakeStrong(this),
            key,
            location,
            chunkId,
            Passed(std::move(sessionCounterGuard)),
            Passed(std::move(lockedChunkGuard)),
            artifactDownloadOptions,
            chunkReadOptions,
            Passed(std::move(cookie))));
    }

    void DoValidateArtifact(
        TInsertCookie cookie,
        const TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions,
        const TClientChunkReadOptions& chunkReadOptions,
        const TRegisteredChunkDescriptor& descriptor,
        NLogging::TLogger Logger)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto chunkId = descriptor.Descriptor.Id;
        const auto& location = descriptor.Location;

        Logger = Logger.WithTag("ChunkId: %v", chunkId);

        if (!CanPrepareSingleChunk(key)) {
            YT_LOG_INFO("Skipping validation for multi-chunk artifact");
            auto chunk = CreateChunk(location, key, descriptor.Descriptor);
            cookie.EndInsert(chunk);
            return;
        }

        YT_LOG_INFO("Scheduling cached chunk validation");
        location->GetAuxPoolInvoker()->Invoke(BIND(
            &TImpl::DoValidateChunk,
            MakeStrong(this),
            Passed(std::move(cookie)),
            key,
            artifactDownloadOptions,
            chunkReadOptions,
            descriptor,
            Logger));
    }

    void DoValidateChunk(
        TInsertCookie cookie,
        const TArtifactKey& key,
        const TArtifactDownloadOptions& artifactDownloadOptions,
        const TClientChunkReadOptions& chunkReadOptions,
        const TRegisteredChunkDescriptor& descriptor,
        const NLogging::TLogger& Logger)
    {
        // NB(psushin): cached chunks (non-artifacts) are not fsynced when written. This may result in truncated or even empty
        // files on power loss. To detect corrupted chunks we validate their size against value in misc extension.
        auto chunkId = descriptor.Descriptor.Id;
        const auto& location = descriptor.Location;

        VERIFY_INVOKER_AFFINITY(location->GetAuxPoolInvoker());

        try {
            YT_LOG_INFO("Chunk validation started");

            auto dataFileName = location->GetChunkPath(chunkId);

            auto chunkReader = New<TChunkFileReader>(
                location->GetIOEngine(),
                chunkId,
                dataFileName);

            TClientChunkReadOptions chunkReadOptions{
                .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::Idle, 0, TInstant::Zero(), {"Validate chunk length"}),
                .ReadSessionId = TReadSessionId::Create()
            };

            auto metaOrError = WaitFor(chunkReader->GetMeta(chunkReadOptions));
            THROW_ERROR_EXCEPTION_IF_FAILED(metaOrError, "Failed to read cached chunk meta");

            const auto& meta = *metaOrError.Value();
            auto miscExt = GetProtoExtension<TMiscExt>(meta.extensions());

            try {
                TFile dataFile(dataFileName, OpenExisting|RdOnly|CloseOnExec);
                if (dataFile.GetLength() != miscExt.compressed_data_size()) {
                    THROW_ERROR_EXCEPTION("Chunk length mismatch")
                        << TErrorAttribute("chunk_id", chunkId)
                        << TErrorAttribute("expected_size", miscExt.compressed_data_size())
                        << TErrorAttribute("actual_size", dataFile.GetLength());
                }
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to validate cached chunk size")
                    << ex;
            }

            YT_LOG_INFO("Chunk validation completed");

            auto chunk = CreateChunk(location, key, descriptor.Descriptor);
            cookie.EndInsert(chunk);
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Chunk is corrupted");

            location->RemoveChunkFiles(chunkId, true);

            DoDownloadArtifact(
                std::move(cookie),
                key,
                artifactDownloadOptions,
                chunkReadOptions,
                Logger);
        }
    }


    void OnChunkCreated(
        const TCacheLocationPtr& location,
        const TChunkDescriptor& descriptor)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Cached chunk object created (ChunkId: %v, LocationId: %v)",
            descriptor.Id,
            location->GetId());

        location->UpdateChunkCount(+1);
        location->UpdateUsedSpace(+descriptor.DiskSpace);
    }

    void OnChunkDestroyed(
        const TCacheLocationPtr& location,
        const TChunkDescriptor& descriptor)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Cached chunk object destroyed (ChunkId: %v, LocationId: %v)",
            descriptor.Id,
            location->GetId());

        location->UpdateChunkCount(-1);
        location->UpdateUsedSpace(-descriptor.DiskSpace);

        location->GetAuxPoolInvoker()->Invoke(BIND(
            &TCacheLocation::RemoveChunkFiles,
            location,
            descriptor.Id,
            true));
    }


    TCachedBlobChunkPtr CreateChunk(
        const TCacheLocationPtr& location,
        const TArtifactKey& key,
        const TChunkDescriptor& descriptor,
        TLockedChunkGuard&& lockedChunkGuard = {},
        const NChunkClient::TRefCountedChunkMetaPtr& meta = nullptr)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto chunk = New<TCachedBlobChunk>(
            TChunkContext::Create(Bootstrap_),
            location,
            descriptor,
            meta,
            key,
            BIND(&TImpl::OnChunkDestroyed, MakeStrong(this), location, descriptor));
        OnChunkCreated(location, descriptor);
        lockedChunkGuard.Release();
        return chunk;
    }

    void RegisterChunk(
        const TCacheLocationPtr& location,
        const TChunkDescriptor& descriptor)
    {
        VERIFY_INVOKER_AFFINITY(location->GetAuxPoolInvoker());

        auto chunkId = descriptor.Id;

        auto optionalKey = TryParseArtifactMeta(location, chunkId);
        if (!optionalKey) {
            return;
        }
        const auto& key = *optionalKey;

        {
            auto lockedChunkGuard = location->TryLockChunk(chunkId);
            YT_VERIFY(lockedChunkGuard);
            lockedChunkGuard.Release();
        }

        bool inserted;
        {
            auto guard = Guard(RegisteredChunkMapLock_);
            inserted = RegisteredChunkMap_.emplace(key, TRegisteredChunkDescriptor{
                .Location = location,
                .Descriptor = descriptor
            }).second;
        }

        if (!inserted) {
            YT_LOG_WARNING("Removing duplicate cached chunk (ChunkId: %v)",
                chunkId);
            location->RemoveChunkFiles(chunkId, true);
            return;
        }

        YT_LOG_DEBUG("Cached chunk registered (ChunkId: %v, LocationId: %v, DiskSpace: %v)",
            chunkId,
            location->GetId(),
            descriptor.DiskSpace);
    }


    i64 GetWeight(const TCachedBlobChunkPtr& chunk) const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return chunk->GetInfo().disk_space();
    }

    void OnAdded(const TCachedBlobChunkPtr& chunk) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Chunk object added to cache (ChunkId: %v, LocationId: %v)",
            chunk->GetId(),
            chunk->GetLocation()->GetId());

        TAsyncSlruCacheBase::OnAdded(chunk);
    }

    void OnRemoved(const TCachedBlobChunkPtr& chunk) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        YT_LOG_DEBUG("Chunk object removed from cache (ChunkId: %v, LocationId: %v)",
            chunk->GetId(),
            chunk->GetLocation()->GetId());

        TAsyncSlruCacheBase::OnRemoved(chunk);
    }


    std::tuple<TCacheLocationPtr, TLockedChunkGuard> AcquireNewChunkLocation(TChunkId chunkId) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        struct TCandidate
        {
            TCacheLocationPtr Location;
            int SessionCount;
            i64 AvailableSpace;
        };
        std::vector<TCandidate> candidates;
        for (const auto& location : Locations_) {
            if (location->IsEnabled()) {
                TCandidate candidate{
                    .Location = location,
                    .SessionCount = location->GetSessionCount(),
                    .AvailableSpace = location->GetAvailableSpace()
                };
                candidates.push_back(candidate);
            }
        }

        SortBy(candidates, [] (const TCandidate& candidate) {
            return std::pair(candidate.SessionCount, -candidate.AvailableSpace);
        });

        for (const auto& candidate : candidates) {
            const auto& location = candidate.Location;
            if (auto guard = location->TryLockChunk(chunkId)) {
                return {location, std::move(guard)};
            }
        }

        return {nullptr, TLockedChunkGuard()};
    }

    static TChunkId GetOrCreateArtifactId(const TArtifactKey& key, bool canPrepareSingleChunk)
    {
        if (canPrepareSingleChunk) {
            YT_VERIFY(key.chunk_specs_size() == 1);
            const auto& chunkSpec = key.chunk_specs(0);
            return FromProto<TChunkId>(chunkSpec.chunk_id());
        } else {
            return TChunkId(
                static_cast<ui32>(TInstant::Now().MicroSeconds()),
                static_cast<ui32>(EObjectType::Artifact),
                RandomNumber<ui32>(),
                RandomNumber<ui32>());
        }
    }

    static bool CanPrepareSingleChunk(const TArtifactKey& key)
    {
        if (CheckedEnumCast<EDataSourceType>(key.data_source().type()) != EDataSourceType::File) {
            return false;
        }
        if (key.chunk_specs_size() != 1) {
            return false;
        }
        if (!key.format().empty()) {
            return false;
        }

        const auto& chunk = key.chunk_specs(0);
        if (chunk.has_lower_limit() && !IsTrivial(chunk.lower_limit())) {
            return false;
        }
        if (chunk.has_upper_limit() && !IsTrivial(chunk.upper_limit())) {
            return false;
        }

        auto miscExt = GetProtoExtension<TMiscExt>(chunk.chunk_meta().extensions());
        NCompression::ECodec compressionCodecId;
        if (!TryEnumCast(miscExt.compression_codec(), &compressionCodecId) ||
            compressionCodecId != NCompression::ECodec::None)
        {
            return false;
        }

        auto chunkId = FromProto<TChunkId>(chunk.chunk_id());
        if (IsErasureChunkId(chunkId)) {
            return false;
        }

        return true;
    }


    TClientChunkReadOptions MakeClientChunkReadOptions(
        TArtifactDownloadOptions artifactDownloadOptions,
        bool bypassArtifactCache)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto workloadDescriptor = GetArtifactCacheReaderConfig()->WorkloadDescriptor;
        auto& annotations = workloadDescriptor.Annotations;
        annotations = artifactDownloadOptions.WorkloadDescriptorAnnotations;
        annotations.push_back("Type: ChunkCache");
        annotations.push_back(Format("BypassArtifactCache: %v", bypassArtifactCache));

        return TClientChunkReadOptions{
            .WorkloadDescriptor = workloadDescriptor,
            .ReadSessionId = TReadSessionId::Create()
        };
    }


    void DownloadChunk(
        const TArtifactKey& key,
        const TCacheLocationPtr& location,
        TChunkId chunkId,
        TSessionCounterGuard /*sessionCounterGuard*/,
        TLockedChunkGuard lockedChunkGuard,
        const TArtifactDownloadOptions& artifactDownloadOptions,
        const TClientChunkReadOptions& chunkReadOptions,
        TInsertCookie cookie)
    {
        VERIFY_INVOKER_AFFINITY(location->GetAuxPoolInvoker());

        const auto& chunkSpec = key.chunk_specs(0);
        auto seedReplicas = GetReplicasFromChunkSpec(chunkSpec);

        auto Logger = ExecNodeLogger.WithTag("ChunkId: %v, ReadSessionId: %v, Location: %v",
            chunkId,
            chunkReadOptions.ReadSessionId,
            location->GetId());

        try {
            auto remoteReaderOptions = New<TRemoteReaderOptions>();
            remoteReaderOptions->EnableP2P = true;

            auto artifactCacheReaderConfig = GetArtifactCacheReaderConfig();

            auto traceContext = CreateTraceContextFromCurrent("ChunkReader");
            TTraceContextGuard guard(traceContext);

            PackBaggageFromDataSource(traceContext, FromProto<NChunkClient::TDataSource>(key.data_source()));

            auto chunkReaderHost = New<TChunkReaderHost>(
                Bootstrap_->GetClient(),
                Bootstrap_->GetLocalDescriptor(),
                Bootstrap_->GetBlockCache(),
                /*chunkMetaCache*/ nullptr,
                /*nodeStatusDirectory*/ nullptr,
                Bootstrap_->GetThrottler(EExecNodeThrottlerKind::ArtifactCacheIn),
                Bootstrap_->GetReadRpsOutThrottler(),
                artifactDownloadOptions.TrafficMeter);

            auto chunkReader = CreateReplicationReader(
                artifactCacheReaderConfig,
                remoteReaderOptions,
                std::move(chunkReaderHost),
                chunkId,
                seedReplicas);

            auto fileName = location->GetChunkPath(chunkId);
            auto chunkWriter = New<TChunkFileWriter>(
                location->GetIOEngine(),
                chunkId,
                fileName,
                /*syncOnClose*/ false);

            auto checkedChunkWriter = New<TErrorInterceptingChunkWriter>(location, chunkWriter);

            YT_LOG_DEBUG("Opening chunk writer");

            WaitFor(checkedChunkWriter->Open())
                .ThrowOnError();

            YT_LOG_DEBUG("Getting chunk meta");

            auto chunkMeta = WaitFor(chunkReader->GetMeta(
                chunkReadOptions))
                .ValueOrThrow();

            // Download all blocks.
            auto blocksExt = GetProtoExtension<NChunkClient::NProto::TBlocksExt>(chunkMeta->extensions());
            int blockCount = blocksExt.blocks_size();
            std::vector<TBlockFetcher::TBlockInfo> blocks;
            blocks.reserve(blockCount);
            for (int index = 0; index < blockCount; ++index) {
                blocks.push_back({
                    .ReaderIndex = 0,
                    .BlockIndex = index,
                    .Priority = index,
                    .UncompressedDataSize = blocksExt.blocks(index).size(),
                    .BlockType = EBlockType::None,
                });
            }

            auto memoryManagerHolder = TChunkReaderMemoryManager::CreateHolder(
                TChunkReaderMemoryManagerOptions(artifactCacheReaderConfig->WindowSize));

            i64 requiredMemory = 0;
            for (const auto& block : blocks) {
                requiredMemory = std::max(requiredMemory, block.UncompressedDataSize);
            }
            memoryManagerHolder->Get()->SetRequiredMemorySize(requiredMemory);

            auto blockFetcher = New<TBlockFetcher>(
                artifactCacheReaderConfig,
                std::move(blocks),
                std::move(memoryManagerHolder),
                std::vector<IChunkReaderPtr>({chunkReader}),
                GetNullBlockCache(),
                NCompression::ECodec::None,
                /*compressionRatio*/ 1.0,
                chunkReadOptions);
            blockFetcher->Start();

            for (int index = 0; index < blockCount; ++index) {
                YT_LOG_DEBUG("Downloading block (BlockIndex: %v)",
                    index);

                auto block = WaitFor(blockFetcher->FetchBlock(index))
                    .ValueOrThrow();

                YT_LOG_DEBUG("Writing block (BlockIndex: %v)",
                    index);

                if (!checkedChunkWriter->WriteBlock(chunkReadOptions.WorkloadDescriptor, block)) {
                    WaitFor(chunkWriter->GetReadyEvent())
                        .ThrowOnError();
                }

                WaitFor(location->GetInThrottler()->Throttle(block.Size()))
                    .ThrowOnError();
            }

            YT_LOG_DEBUG("Closing chunk");

            auto deferredChunkMeta = New<TDeferredChunkMeta>();
            deferredChunkMeta->CopyFrom(*chunkMeta);

            WaitFor(checkedChunkWriter->Close(chunkReadOptions.WorkloadDescriptor, deferredChunkMeta))
                .ThrowOnError();

            if (Bootstrap_->GetIOTracker()->IsEnabled()) {
                Bootstrap_->GetIOTracker()->Enqueue(
                    TIOCounters{
                        .Bytes = checkedChunkWriter->GetChunkInfo().disk_space(),
                        .IORequests = 1,
                    },
                    /*tags*/ {
                        {FormatIOTag(ERawIOTag::LocationId), ToString(location->GetId())},
                        {FormatIOTag(EAggregateIOTag::LocationType), FormatEnum(location->GetType())},
                        {FormatIOTag(EAggregateIOTag::Medium), location->GetMediumName()},
                        {FormatIOTag(EAggregateIOTag::DiskFamily), location->GetDiskFamily()},
                        {FormatIOTag(EAggregateIOTag::Direction), "write"},
                        {FormatIOTag(EAggregateIOTag::User), GetCurrentAuthenticationIdentity().User},
                        {FormatIOTag(ERawIOTag::ChunkId), ToString(chunkId)},
                    });
            }

            YT_LOG_INFO("Chunk is downloaded into cache");

            TChunkDescriptor descriptor(chunkId, chunkWriter->GetChunkInfo().disk_space());
            auto chunk = CreateChunk(location,  key, descriptor, std::move(lockedChunkGuard), chunkMeta);
            cookie.EndInsert(chunk);
        } catch (const std::exception& ex) {
            auto error = TError("Error downloading chunk %v into cache",
                chunkId)
                << ex;
            cookie.Cancel(error);
            YT_LOG_WARNING(error);
        }
    }

    void DownloadFile(
        const TArtifactKey& key,
        const TCacheLocationPtr& location,
        TChunkId chunkId,
        TSessionCounterGuard /*sessionCounterGuard*/,
        TLockedChunkGuard lockedChunkGuard,
        const TArtifactDownloadOptions& artifactDownloadOptions,
        const TClientChunkReadOptions& chunkReadOptions,
        TInsertCookie cookie)
    {
        VERIFY_INVOKER_AFFINITY(location->GetAuxPoolInvoker());

        try {
            auto producer = MakeFileProducer(
                key,
                artifactDownloadOptions.TrafficMeter,
                chunkReadOptions,
                location->GetInThrottler());

            auto chunk = ProduceArtifactFile(
                key,
                location,
                chunkId,
                std::move(lockedChunkGuard),
                producer);
            cookie.EndInsert(chunk);
        } catch (const std::exception& ex) {
            auto error = TError("Error downloading file artifact into cache")
                << ex;
            cookie.Cancel(error);
            YT_LOG_WARNING(error);
        }
    }

    std::function<void(IOutputStream*)> MakeFileProducer(
        const TArtifactKey& key,
        const TTrafficMeterPtr& trafficMeter,
        const TClientChunkReadOptions& chunkReadOptions,
        const IThroughputThrottlerPtr& throttler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::vector<TChunkSpec> chunkSpecs(key.chunk_specs().begin(), key.chunk_specs().end());

        auto readerOptions = New<TMultiChunkReaderOptions>();
        readerOptions->EnableP2P = true;

        auto chunkReaderHost = New<TChunkReaderHost>(
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalDescriptor(),
            Bootstrap_->GetBlockCache(),
            /*chunkMetaCache*/ nullptr,
            /*nodeStatusDirectory*/ nullptr,
            Bootstrap_->GetThrottler(EExecNodeThrottlerKind::ArtifactCacheIn),
            Bootstrap_->GetReadRpsOutThrottler(),
            trafficMeter);
        auto reader = CreateFileMultiChunkReader(
            GetArtifactCacheReaderConfig(),
            readerOptions,
            std::move(chunkReaderHost),
            chunkReadOptions,
            chunkSpecs,
            FromProto<NChunkClient::TDataSource>(key.data_source()));

        return [=] (IOutputStream* output) {
            TBlock block;
            while (reader->ReadBlock(&block)) {
                if (block.Data.Empty()) {
                    WaitFor(reader->GetReadyEvent())
                        .ThrowOnError();
                } else {
                    output->Write(block.Data.Begin(), block.Size());
                    WaitFor(throttler->Throttle(block.Size()))
                        .ThrowOnError();
                }
            }
        };
    }

    void DownloadTable(
        const TArtifactKey& key,
        const TCacheLocationPtr& location,
        TChunkId chunkId,
        TSessionCounterGuard /*sessionCounterGuard*/,
        TLockedChunkGuard lockedChunkGuard,
        const TArtifactDownloadOptions& artifactDownloadOptions,
        const TClientChunkReadOptions& chunkReadOptions,
        TInsertCookie cookie)
    {
        VERIFY_INVOKER_AFFINITY(location->GetAuxPoolInvoker());

        try {
            auto producer = MakeTableProducer(
                key,
                artifactDownloadOptions.TrafficMeter,
                chunkReadOptions,
                location->GetInThrottler());

            auto chunk = ProduceArtifactFile(
                key,
                location,
                chunkId,
                std::move(lockedChunkGuard),
                producer);
            cookie.EndInsert(chunk);
        } catch (const std::exception& ex) {
            auto error = TError("Error downloading table artifact into cache")
                << ex;
            cookie.Cancel(error);
            YT_LOG_WARNING(error);
        }
    }

    // COMPAT(levysotsky): This function is to be removed after both CA and nodes are updated.
    // See YT-16507.
    static NTableClient::TTableSchemaPtr SetStableNames(
        const NTableClient::TTableSchemaPtr& schema,
        const NTableClient::TColumnRenameDescriptors& renameDescriptors)
    {
        THashMap<TString, TString> nameToStableName;
        for (const auto& renameDescriptor : renameDescriptors) {
            nameToStableName.emplace(renameDescriptor.NewName, renameDescriptor.OriginalName);
        }

        std::vector<NTableClient::TColumnSchema> columns;
        for (const auto& originalColumn : schema->Columns()) {
            auto& column = columns.emplace_back(originalColumn);
            YT_VERIFY(!column.IsRenamed());
            if (auto it = nameToStableName.find(column.Name())) {
                column.SetStableName(NTableClient::TColumnStableName(it->second));
            }
        }
        return New<NTableClient::TTableSchema>(
            std::move(columns),
            schema->GetStrict(),
            schema->GetUniqueKeys(),
            schema->GetSchemaModification());
    }

    std::function<void(IOutputStream*)> MakeTableProducer(
        const TArtifactKey& key,
        const TTrafficMeterPtr& trafficMeter,
        const TClientChunkReadOptions& chunkReadOptions,
        const IThroughputThrottlerPtr& throttler)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto nameTable = New<TNameTable>();

        auto readerOptions = New<NTableClient::TTableReaderOptions>();
        readerOptions->EnableP2P = true;

        std::vector<TDataSliceDescriptor> dataSliceDescriptors;
        auto dataSourceDirectory = New<NChunkClient::TDataSourceDirectory>();

        NChunkClient::TDataSource dataSource;
        FromProto(&dataSource, key.data_source());

        if (dataSource.Schema() && !dataSource.Schema()->HasRenamedColumns()) {
            dataSource.Schema() = SetStableNames(
                dataSource.Schema(),
                dataSource.ColumnRenameDescriptors());
        }

        dataSourceDirectory->DataSources().push_back(dataSource);

        switch (dataSource.GetType()) {
            case EDataSourceType::UnversionedTable:
                for (const auto& chunkSpec : key.chunk_specs()) {
                    dataSliceDescriptors.push_back(TDataSliceDescriptor(chunkSpec));
                }
                break;

            case EDataSourceType::VersionedTable:
                dataSliceDescriptors.push_back(TDataSliceDescriptor(FromProto<std::vector<TChunkSpec>>(key.chunk_specs())));
                break;

            default:
                YT_ABORT();
        }

        auto chunkReaderHost = New<TChunkReaderHost>(
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalDescriptor(),
            Bootstrap_->GetBlockCache(),
            /*chunkMetaCache*/ nullptr,
            /*nodeStatusDirectory*/ nullptr,
            Bootstrap_->GetThrottler(EExecNodeThrottlerKind::ArtifactCacheIn),
            Bootstrap_->GetReadRpsOutThrottler(),
            trafficMeter);
        auto reader = CreateSchemalessSequentialMultiReader(
            GetArtifactCacheReaderConfig(),
            readerOptions,
            std::move(chunkReaderHost),
            dataSourceDirectory,
            std::move(dataSliceDescriptors),
            /*hintKeys*/ std::nullopt,
            nameTable,
            chunkReadOptions,
            TReaderInterruptionOptions::InterruptibleWithEmptyKey(),
            /*columnFilter*/ {},
            /*partitionTag*/ std::nullopt);

        auto schema = dataSource.Schema();
        auto format = ConvertTo<NFormats::TFormat>(TYsonString(key.format()));

        return [=] (IOutputStream* output) {
            auto writer = CreateStaticTableWriterForFormat(
                format,
                nameTable,
                {schema ? schema : New<TTableSchema>()},
                CreateAsyncAdapter(output),
                false, /*enableContextSaving*/
                New<TControlAttributesConfig>(),
                0);
            TPipeReaderToWriterOptions options;
            options.BufferRowCount = TableArtifactBufferRowCount;
            options.Throttler = throttler;
            PipeReaderToWriter(
                reader,
                writer,
                options);
        };
    }

    TCachedBlobChunkPtr ProduceArtifactFile(
        const TArtifactKey& key,
        const TCacheLocationPtr& location,
        TChunkId chunkId,
        TLockedChunkGuard lockedChunkGuard,
        const std::function<void(IOutputStream*)>& producer)
    {
        VERIFY_INVOKER_AFFINITY(location->GetAuxPoolInvoker());

        YT_LOG_INFO("Producing artifact file (ChunkId: %v, Location: %v)",
            chunkId,
            location->GetId());

        auto dataFileName = location->GetChunkPath(chunkId);
        auto metaFileName = dataFileName + ArtifactMetaSuffix;
        auto tempDataFileName = dataFileName + NFS::TempFileSuffix;
        auto tempMetaFileName = metaFileName + NFS::TempFileSuffix;

        auto metaBlob = SerializeProtoToRef(key);
        TArtifactMetaHeader metaHeader;

        std::unique_ptr<TFile> tempDataFile;
        std::unique_ptr<TFile> tempMetaFile;
        i64 chunkSize;

        location->DisableOnError(BIND([&] {
            tempDataFile = std::make_unique<TFile>(
                tempDataFileName,
                CreateAlways | WrOnly | Seq | CloseOnExec);
            tempDataFile->Flock(LOCK_EX);

            tempMetaFile = std::make_unique<TFile>(
                tempMetaFileName,
                CreateAlways | WrOnly | Seq | CloseOnExec);
            tempMetaFile->Flock(LOCK_EX);
        })).Run();

        TUnbufferedFileOutput fileOutput(*tempDataFile);
        TErrorInterceptingOutput checkedOutput(location, &fileOutput);

        auto traceContext = CreateTraceContextFromCurrent("ChunkCache");
        TTraceContextGuard guard(traceContext);

        PackBaggageFromDataSource(traceContext, FromProto<NChunkClient::TDataSource>(key.data_source()));

        producer(&checkedOutput);

        if (Bootstrap_->GetIOTracker()->IsEnabled()) {
            Bootstrap_->GetIOTracker()->Enqueue(
                TIOCounters{
                    .Bytes = tempDataFile->GetLength(),
                    .IORequests = 1,
                },
                /*tags*/ {
                    {FormatIOTag(ERawIOTag::LocationId), ToString(location->GetId())},
                    {FormatIOTag(EAggregateIOTag::LocationType), FormatEnum(location->GetType())},
                    {FormatIOTag(EAggregateIOTag::Medium), location->GetMediumName()},
                    {FormatIOTag(EAggregateIOTag::DiskFamily), location->GetDiskFamily()},
                    {FormatIOTag(EAggregateIOTag::Direction), "write"},
                    {FormatIOTag(EAggregateIOTag::User), GetCurrentAuthenticationIdentity().User},
                    {FormatIOTag(ERawIOTag::ChunkId), ToString(chunkId)},
                });
        }

        location->DisableOnError(BIND([&] {
            chunkSize = tempDataFile->GetLength();
            tempDataFile->Flush();
            tempDataFile->Close();

            tempMetaFile->Write(static_cast<void*>(&metaHeader), sizeof(TArtifactMetaHeader));
            tempMetaFile->Write(metaBlob.Begin(), metaBlob.Size());
            tempMetaFile->Flush();
            tempMetaFile->Close();

            NFS::Rename(tempMetaFileName, metaFileName);
            NFS::Rename(tempDataFileName, dataFileName);
        })).Run();

        TChunkDescriptor descriptor(chunkId);
        descriptor.DiskSpace = chunkSize + metaBlob.Size();
        return CreateChunk(location, key, descriptor, std::move(lockedChunkGuard));
    }

    std::optional<TArtifactKey> TryParseArtifactMeta(
        const TCacheLocationPtr& location,
        TChunkId chunkId)
    {
        VERIFY_INVOKER_AFFINITY(location->GetAuxPoolInvoker());

        if (!IsArtifactChunkId(chunkId)) {
            return TArtifactKey(chunkId);
        }

        auto dataFileName = location->GetChunkPath(chunkId);
        auto metaFileName = dataFileName + ArtifactMetaSuffix;

        TSharedMutableRef metaBlob;

        location->DisableOnError(BIND([&] {
            TFile metaFile(
                metaFileName,
                OpenExisting | RdOnly | Seq | CloseOnExec);
            TFileInput metaInput(metaFile);
            metaBlob = TSharedMutableRef::Allocate<TArtifactReaderMetaBufferTag>(metaFile.GetLength());
            metaInput.Read(metaBlob.Begin(), metaFile.GetLength());
        })).Run();

        auto readMeta = [&] () -> std::optional<TArtifactKey> {
            if (metaBlob.Size() < sizeof(TArtifactMetaHeader)) {
                YT_LOG_WARNING("Artifact meta file %v is too short: at least %v bytes expected",
                    metaFileName,
                    sizeof(TArtifactMetaHeader));
                return std::nullopt;
            }

            const auto* header = reinterpret_cast<const TArtifactMetaHeader*>(metaBlob.Begin());
            if (header->Signature != header->ExpectedSignature) {
                YT_LOG_WARNING("Bad signature in artifact meta file %v: expected %X, actual %X",
                    metaFileName,
                    header->ExpectedSignature,
                    header->Signature);
                return std::nullopt;
            }

            if (header->Version != header->ExpectedVersion) {
                YT_LOG_WARNING("Incompatible version in artifact meta file %v: expected %v, actual %v",
                    metaFileName,
                    header->ExpectedVersion,
                    header->Version);
                return std::nullopt;
            }

            metaBlob = metaBlob.Slice(sizeof(TArtifactMetaHeader), metaBlob.Size());
            TArtifactKey key;
            if (!TryDeserializeProto(&key, metaBlob)) {
                YT_LOG_WARNING("Failed to parse artifact meta file %v",
                    metaFileName);
                return std::nullopt;
            }

            return key;
        };

        auto key = readMeta();
        if (!key) {
            location->RemoveChunkFiles(chunkId, true);
        }
        return key;
    }

    TArtifactCacheReaderConfigPtr GetArtifactCacheReaderConfig() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ArtifactCacheReaderConfig_.Acquire();
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldNodeConfig*/,
        const TClusterNodeDynamicConfigPtr& newNodeConfig)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        ArtifactCacheReaderConfig_.Store(newNodeConfig->DataNode->ArtifactCacheReader);
    }
};

////////////////////////////////////////////////////////////////////////////////

TChunkCache::TChunkCache(TDataNodeConfigPtr config, IBootstrap* bootstrap)
    : Impl_(New<TImpl>(config, bootstrap))
{ }

TChunkCache::~TChunkCache() = default;

void TChunkCache::Initialize()
{
    Impl_->Initialize();
}

bool TChunkCache::IsEnabled() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->IsEnabled();
}

IChunkPtr TChunkCache::FindChunk(TChunkId chunkId)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->FindChunk(chunkId);
}

std::vector<IChunkPtr> TChunkCache::GetChunks()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->GetChunks();
}

int TChunkCache::GetChunkCount()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->GetSize();
}

TFuture<void> TChunkCache::RemoveChunksByLocation(const TCacheLocationPtr& location)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->RemoveChunksByLocation(location);
}

TFuture<IChunkPtr> TChunkCache::DownloadArtifact(
    const TArtifactKey& key,
    const TArtifactDownloadOptions& artifactDownloadOptions,
    bool* fetchedFromCache)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->DownloadArtifact(key, artifactDownloadOptions, fetchedFromCache);
}

std::function<void(IOutputStream*)> TChunkCache::MakeArtifactDownloadProducer(
    const TArtifactKey& key,
    const TArtifactDownloadOptions& artifactDownloadOptions)
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Impl_->MakeArtifactDownloadProducer(key, artifactDownloadOptions);
}

DELEGATE_BYREF_RO_PROPERTY(TChunkCache, std::vector<TCacheLocationPtr>, Locations, *Impl_);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
