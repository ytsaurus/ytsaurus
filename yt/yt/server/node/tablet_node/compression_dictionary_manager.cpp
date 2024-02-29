#include "compression_dictionary_manager.h"

#include "bootstrap.h"
#include "tablet.h"
#include "hint_manager.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/core/compression/dictionary_codec.h>

#include <yt/yt/core/misc/async_slru_cache.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NCompression;
using namespace NProfiling;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

using TRowDigestedCompressionDictionary = THashMap<int, IDigestedCompressionDictionaryPtr>;
using TRowDigestedDecompressionDictionary = THashMap<int, IDigestedDecompressionDictionaryPtr>;
using TRowDigestedDictionary = std::variant<
    TRowDigestedCompressionDictionary,
    TRowDigestedDecompressionDictionary>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDictionaryCompressionSession)

class TDictionaryCompressionSession
    : public IDictionaryCompressionSession
{
public:
    TDictionaryCompressionSession(
        const TTabletSnapshotPtr& tabletSnapshot,
        TRowDictionaryCompressors rowDictionaryCompressors)
        : Schema_(tabletSnapshot->PhysicalSchema)
        , ProbationSize_(tabletSnapshot->Settings.MountConfig
            ->ValueDictionaryCompression->PolicyProbationSamplesSize)
        , MaxAcceptableCompressionRatio_(tabletSnapshot->Settings.MountConfig
            ->ValueDictionaryCompression->MaxAcceptableCompressionRatio)
        , Logger(TabletNodeLogger.WithTag("%v",
            tabletSnapshot->LoggingTag))
        , RowDictionaryCompressors_(std::move(rowDictionaryCompressors))
    { }

    bool FeedSample(TVersionedRow row, TChunkedMemoryPool* pool) override
    {
        YT_VERIFY(!ElectedPolicy_);

        TWallTimer compressionTimer;
        for (const auto& value : row.Values()) {
            if (!IsValueCompressable(value)) {
                continue;
            }

            for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
                auto& columnCompressor = GetOrCrash(
                    RowDictionaryCompressors_[policy].ColumnCompressors,
                    value.Id);
                if (columnCompressor.Compressor) {
                    auto compressedRef = columnCompressor.Compressor->Compress(
                        pool,
                        TRef(value.Data.String, value.Length));
                    columnCompressor.CompressedSamplesSize += compressedRef.Size();
                    // TODO(akozhikhov): Free memory as this ref is never used?
                } else {
                    columnCompressor.CompressedSamplesSize += value.Length;
                }
            }

            ProcessedSamplesSize_ += value.Length;
            ++ProcessedSampleCount_;
        }

        CompressionTime_ += compressionTimer.GetElapsedTime();

        if (ProcessedSamplesSize_ >= ProbationSize_) {
            ElectBestPolicy();
        }

        return !ElectedPolicy_;
    }

    void CompressValuesInRow(TMutableVersionedRow* row, TChunkedMemoryPool* pool) override
    {
        if (!ElectedPolicy_) {
            ElectBestPolicy();
        }

        TWallTimer compressionTimer;
        for (auto& value : row->Values()) {
            if (!IsValueCompressable(value)) {
                continue;
            }

            auto& columnCompressor = GetOrCrash(
                RowDictionaryCompressors_[*ElectedPolicy_].ColumnCompressors,
                value.Id);
            if (!columnCompressor.Compressor) {
                continue;
            }

            PerformedCompression_ = true;
            auto compressedRef = columnCompressor.Compressor->Compress(
                pool,
                TRef(value.Data.String, value.Length));

            value.Data.String = compressedRef.Begin();
            value.Length = compressedRef.Size();
        }

        CompressionTime_ += compressionTimer.GetElapsedTime();
    }

    TChunkId GetCompressionDictionaryId() const override
    {
        if (!ElectedPolicy_ || !PerformedCompression_) {
            return {};
        }

        return RowDictionaryCompressors_[*ElectedPolicy_].DictionaryId;
    }

    TDuration GetCompressionTime() const override
    {
        return CompressionTime_;
    }

private:
    const TTableSchemaPtr Schema_;
    const i64 ProbationSize_;
    const double MaxAcceptableCompressionRatio_;

    const NLogging::TLogger Logger;

    TRowDictionaryCompressors RowDictionaryCompressors_;

    std::optional<EDictionaryCompressionPolicy> ElectedPolicy_;
    i64 ProcessedSamplesSize_ = 0;
    int ProcessedSampleCount_ = 0;

    TDuration CompressionTime_;

    bool PerformedCompression_ = false;


    void ElectBestPolicy()
    {
        YT_VERIFY(!ElectedPolicy_);

        TEnumIndexedArray<EDictionaryCompressionPolicy, i64> policyToCompressedSize;
        for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
            for (const auto& [_, columnCompressors] : RowDictionaryCompressors_[policy].ColumnCompressors) {
                policyToCompressedSize[policy] += columnCompressors.CompressedSamplesSize;
            }
        }

        for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
            if (!ElectedPolicy_ ||
                policyToCompressedSize[policy] < policyToCompressedSize[*ElectedPolicy_])
            {
                ElectedPolicy_ = policy;
            }
        }

        YT_VERIFY(ElectedPolicy_);

        if (policyToCompressedSize[*ElectedPolicy_] >
            policyToCompressedSize[EDictionaryCompressionPolicy::None] * MaxAcceptableCompressionRatio_)
        {
            ElectedPolicy_ = EDictionaryCompressionPolicy::None;
        }

        YT_LOG_DEBUG("Dictionary compression session elected best policy "
            "(Policy: %v, ChunkId: %v, ProcessedSampleCount: %v, ProcessedSamplesSize: %v, PolicyToCompressedSize: %v)",
            ElectedPolicy_,
            RowDictionaryCompressors_[*ElectedPolicy_].DictionaryId,
            ProcessedSampleCount_,
            ProcessedSamplesSize_,
            policyToCompressedSize);
    }

    bool IsValueCompressable(const TVersionedValue& value) const
    {
        if (value.Type == EValueType::Null) {
            return false;
        }

        if (!IsStringLikeType(value.Type)) {
            return false;
        }

        if (!Schema_->Columns()[value.Id].MaxInlineHunkSize()) {
            return false;
        }

        if (Any(value.Flags & EValueFlags::Hunk)) {
            return false;
        }

        return true;
    }
};

DEFINE_REFCOUNTED_TYPE(TDictionaryCompressionSession)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDictionaryDecompressionSession)

class TDictionaryDecompressionSession
    : public IDictionaryDecompressionSession
{
public:
    TDictionaryDecompressionSession(
        TTabletSnapshotPtr tabletSnapshot,
        TWeakPtr<ICompressionDictionaryManager> dictionaryManager)
        : TabletSnapshot_(std::move(tabletSnapshot))
        , DictionaryManager_(dictionaryManager)
        , Logger(TabletNodeLogger.WithTag("%v",
            TabletSnapshot_->LoggingTag))
    { }

    TFuture<std::vector<TSharedRef>> DecompressValues(
        std::vector<TUnversionedValue*> values,
        std::vector<TChunkId> dictionaryIds,
        TClientChunkReadOptions chunkReadOptions) override
    {
        YT_VERIFY(values.size() == dictionaryIds.size());

        YT_VERIFY(!Promise_);
        Promise_ = NewPromise<std::vector<TSharedRef>>();

        Logger.AddTag("ReadSessionId: %v",
            chunkReadOptions.ReadSessionId);

        auto dictionaryManager = DictionaryManager_.Lock();
        if (!dictionaryManager) {
            Promise_.TrySet(TError(NYT::EErrorCode::Canceled, "Dictionary manager was destroyed"));
            return Promise_;
        }

        THashSet<TChunkId> dictionaryIdSet(dictionaryIds.begin(), dictionaryIds.end());
        YT_VERIFY(!dictionaryIdSet.contains(NullChunkId));

        YT_LOG_DEBUG("Dictionary decompression session will fetch decompressors from cache (DictionaryIds: %v)",
            dictionaryIdSet);

        auto decompressorsFuture = dictionaryManager->GetDecompressors(
            TabletSnapshot_,
            chunkReadOptions,
            dictionaryIdSet);

        if (auto maybeDecompressors = decompressorsFuture.TryGetUnique();
            maybeDecompressors && maybeDecompressors->IsOK())
        {
            DoDecompressValues(
                /*decompressedValueCount*/ 0,
                std::move(values),
                std::move(dictionaryIds),
                std::move(chunkReadOptions),
                TErrorOr<THashMap<TChunkId, TRowDictionaryDecompressor>>(std::move(*maybeDecompressors)));
        } else {
            decompressorsFuture.SubscribeUnique(BIND(
                &TDictionaryDecompressionSession::DoDecompressValues,
                MakeStrong(this),
                /*decompressedValueCount*/ 0,
                Passed(std::move(values)),
                Passed(std::move(dictionaryIds)),
                Passed(std::move(chunkReadOptions)))
                .Via(GetCurrentInvoker()));
        }

        return Promise_;
    }

    TDuration GetDecompressionTime() const override
    {
        return DecompressionTime_;
    }

private:
    struct TDecompressionSessionDataTag
    { };

    const TTabletSnapshotPtr TabletSnapshot_;
    const TWeakPtr<ICompressionDictionaryManager> DictionaryManager_;

    NLogging::TLogger Logger;

    TPromise<std::vector<TSharedRef>> Promise_;
    std::vector<TSharedRef> Blobs_;

    TDuration DecompressionTime_;


    void DoDecompressValues(
        int decompressedValueCount,
        std::vector<TUnversionedValue*> values,
        std::vector<TChunkId> dictionaryIds,
        TClientChunkReadOptions chunkReadOptions,
        TErrorOr<THashMap<TChunkId, TRowDictionaryDecompressor>>&& decompressorsOrError)
    {
        // Shortcut.
        if (Promise_.IsSet()) {
            return;
        }

        if (!decompressorsOrError.IsOK()) {
            Promise_.TrySet(TError(decompressorsOrError));
            return;
        }

        const auto& decompressors = decompressorsOrError.Value();

        auto* codec = GetDictionaryCompressionCodec();

        TWallTimer decompressionTimer;

        i64 decompressedSize = 0;
        std::vector<i64> contentSizes;
        auto newDecompressedValueCount = decompressedValueCount;
        while (newDecompressedValueCount < std::ssize(values)) {
            auto dictionaryId = dictionaryIds[newDecompressedValueCount];
            YT_VERIFY(dictionaryId);

            auto* compressedValue = values[newDecompressedValueCount];
            YT_VERIFY(IsStringLikeType(compressedValue->Type));
            YT_VERIFY(None(compressedValue->Flags & EValueFlags::Hunk));

            ++newDecompressedValueCount;

            const auto& rowDecompressor = GetOrCrash(decompressors, dictionaryId);
            if (!rowDecompressor.contains(compressedValue->Id)) {
                contentSizes.push_back(-1);
                continue;
            }

            auto frameInfo = codec->GetFrameInfo(TRef(compressedValue->Data.String, compressedValue->Length));
            contentSizes.push_back(frameInfo.ContentSize);
            decompressedSize += contentSizes.back();

            if (decompressedSize >=
                TabletSnapshot_->Settings.MountConfig->ValueDictionaryCompression->MaxDecompressionBlobSize)
            {
                break;
            }
        }

        auto blob = TSharedMutableRef::Allocate<TDecompressionSessionDataTag>(
            decompressedSize,
            { .InitializeStorage = false });
        Blobs_.push_back(blob);

        i64 currentCompressedSize = 0;
        i64 currentDecompressedSize = 0;
        i64 currentUncompressedSize = 0;
        for (int valueIndex = decompressedValueCount; valueIndex < newDecompressedValueCount; ++valueIndex) {
            auto dictionaryId = dictionaryIds[valueIndex];
            YT_VERIFY(dictionaryId);

            auto* compressedValue = values[valueIndex];
            TRef compressedValueRef(compressedValue->Data.String, compressedValue->Length);

            const auto& rowDecompressor = GetOrCrash(decompressors, dictionaryId);
            auto columnDecompressorIt = rowDecompressor.find(compressedValue->Id);
            if (columnDecompressorIt == rowDecompressor.end()) {
                currentUncompressedSize += compressedValue->Length;
                continue;
            }

            TMutableRef output(
                blob.Begin() + currentDecompressedSize,
                contentSizes[valueIndex - decompressedValueCount]);
            columnDecompressorIt->second->Decompress(compressedValueRef, output);

            currentCompressedSize += compressedValue->Length;
            currentDecompressedSize += output.Size();

            compressedValue->Data.String = output.Begin();
            compressedValue->Length = output.Size();
        }

        YT_VERIFY(currentDecompressedSize == std::ssize(blob));

        YT_LOG_DEBUG("Dictionary decompression session successfully decompressed rows "
            "(DecompressionTime: %v, RowCount: %v/%v, CompressedSize: %v, DecompressedSize: %v, UncompressedSize: %v)",
            decompressionTimer.GetElapsedTime(),
            newDecompressedValueCount,
            values.size(),
            currentCompressedSize,
            currentDecompressedSize,
            currentUncompressedSize);

        DecompressionTime_ += decompressionTimer.GetElapsedTime();

        YT_VERIFY(newDecompressedValueCount <= std::ssize(values));
        if (newDecompressedValueCount == std::ssize(values)) {
            Promise_.TrySet(std::move(Blobs_));
        } else {
            GetCurrentInvoker()->Invoke(BIND(
                &TDictionaryDecompressionSession::DoDecompressValues,
                MakeStrong(this),
                newDecompressedValueCount,
                Passed(std::move(values)),
                Passed(std::move(dictionaryIds)),
                Passed(std::move(chunkReadOptions)),
                Passed(std::move(decompressorsOrError))));
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TDictionaryDecompressionSession)

////////////////////////////////////////////////////////////////////////////////

class TDictionaryCompressionFactory
    : public IDictionaryCompressionFactory
{
public:
    TDictionaryCompressionFactory(
        TWeakPtr<TTabletSnapshot> tabletSnapshot,
        TWeakPtr<ICompressionDictionaryManager> dictionaryManager)
        : TabletSnapshot_(std::move(tabletSnapshot))
        , DictionaryManager_(std::move(dictionaryManager))
    { }

    TFuture<IDictionaryCompressionSessionPtr> MaybeCreateDictionaryCompressionSession(
        const TClientChunkReadOptions& chunkReadOptions) const override
    {
        auto dictionaryManager = DictionaryManager_.Lock();
        auto tabletSnapshot = TabletSnapshot_.Lock();
        if (!dictionaryManager || !tabletSnapshot) {
            auto error = TError(NYT::EErrorCode::Canceled,
                "Unable to compress values due to tablet cell reconfiguration");
            return MakeFuture<IDictionaryCompressionSessionPtr>(error);
        }

        auto compressorsFuture = dictionaryManager->MaybeGetCompressors(
            tabletSnapshot,
            chunkReadOptions);

        // NB: Null in case dictionary compression is disabled or there are no constructed dictinaries yet.
        if (!compressorsFuture) {
            return TFuture<IDictionaryCompressionSessionPtr>{};
        }

        return compressorsFuture
            .ApplyUnique(BIND([
                tabletSnapshot = std::move(tabletSnapshot)
            ] (TRowDictionaryCompressors&& compressors) {
                IDictionaryCompressionSessionPtr session = New<TDictionaryCompressionSession>(
                    tabletSnapshot,
                    std::move(compressors));
                return session;
            }));
    }

    IDictionaryDecompressionSessionPtr CreateDictionaryDecompressionSession() const override
    {
        auto tabletSnapshot = TabletSnapshot_.Lock();
        if (!tabletSnapshot) {
            THROW_ERROR_EXCEPTION(NYT::EErrorCode::Canceled,
                "Unable to decompress values due to tablet cell reconfiguration");
        }

        return New<TDictionaryDecompressionSession>(
            tabletSnapshot,
            DictionaryManager_);
    }

private:
    const TWeakPtr<TTabletSnapshot> TabletSnapshot_;
    const TWeakPtr<ICompressionDictionaryManager> DictionaryManager_ = {};
};

////////////////////////////////////////////////////////////////////////////////

struct TCompressionDictionaryCacheKey
{
    TChunkId ChunkId;
    // Content differs for compression and decompression modes.
    bool IsDecompression;
    // This field is used for proper column id mapping in case of schema alteration.
    TObjectId SchemaId;

    bool operator == (const TCompressionDictionaryCacheKey& other) const
    {
        return
            ChunkId == other.ChunkId &&
            IsDecompression == other.IsDecompression &&
            SchemaId == other.SchemaId;
    }

    explicit operator size_t() const
    {
        return MultiHash(
            ChunkId,
            IsDecompression,
            SchemaId);
    }
};

void FormatValue(TStringBuilderBase* builder, const TCompressionDictionaryCacheKey& key, TStringBuf /*spec*/)
{
    builder->AppendFormat("ChunkId: %v, Mode: %v, SchemaId: %v",
        key.ChunkId,
        key.IsDecompression ? "Decompression" : "Compression",
        key.SchemaId);
}

TString ToString(const TCompressionDictionaryCacheKey& key)
{
    return ToStringViaBuilder(key);
}

void Serialize(const TCompressionDictionaryCacheKey& key, NYT::NYson::IYsonConsumer* consumer)
{
    NYT::NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .Item("chunk_id").Value(key.ChunkId)
            .Item("is_decompression").Value(key.IsDecompression)
            .Item("schema_id").Value(key.SchemaId)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCompressionDictionaryCacheEntry)

class TCompressionDictionaryCacheEntry
    : public TAsyncCacheValueBase<TCompressionDictionaryCacheKey, TCompressionDictionaryCacheEntry>
{
public:
    TCompressionDictionaryCacheEntry(
        TCompressionDictionaryCacheKey key,
        TRowDigestedDictionary rowDictionary,
        EDictionaryCompressionPolicy policy)
        : TAsyncCacheValueBase(key)
        , RowDictionary_(std::move(rowDictionary))
        , Policy_(policy)
    { }

    const TRowDigestedCompressionDictionary& GetRowDigestedCompressionDictionary() const
    {
        YT_VERIFY(!GetKey().IsDecompression);
        return std::get<0>(RowDictionary_);
    }

    const TRowDigestedDecompressionDictionary& GetRowDigestedDecompressionDictionary() const
    {
        YT_VERIFY(GetKey().IsDecompression);
        return std::get<1>(RowDictionary_);
    }

    EDictionaryCompressionPolicy GetPolicy() const
    {
        YT_VERIFY(!GetKey().IsDecompression);
        return Policy_;
    }

    i64 GetMemoryUsage() const
    {
        i64 memoryUsage = 0;
        if (GetKey().IsDecompression) {
            for (const auto& [_, digestedDictionary] : std::get<1>(RowDictionary_)) {
                memoryUsage += digestedDictionary->GetMemoryUsage();
            }
        } else {
            for (const auto& [_, digestedDictionary] : std::get<0>(RowDictionary_)) {
                memoryUsage += digestedDictionary->GetMemoryUsage();
            }
        }

        return memoryUsage;
    }

private:
    const TRowDigestedDictionary RowDictionary_;
    const EDictionaryCompressionPolicy Policy_;
};

DEFINE_REFCOUNTED_TYPE(TCompressionDictionaryCacheEntry)

////////////////////////////////////////////////////////////////////////////////

class TCompressionDictionaryManager
    : public ICompressionDictionaryManager
    , public TAsyncSlruCacheBase<TCompressionDictionaryCacheKey, TCompressionDictionaryCacheEntry>
{
public:
    TCompressionDictionaryManager(
        TSlruCacheConfigPtr config,
        IBootstrap* bootstrap)
        : TAsyncSlruCacheBase(
            std::move(config),
            TabletNodeProfiler.WithPrefix("/compression_dictionary_cache"))
        , Bootstrap_(bootstrap)
    { }

    void OnDynamicConfigChanged(const TSlruCacheDynamicConfigPtr& config) override
    {
        TAsyncSlruCacheBase<TCompressionDictionaryCacheKey, TCompressionDictionaryCacheEntry>::Reconfigure(config);
    }

    IDictionaryCompressionFactoryPtr CreateTabletDictionaryCompressionFactory(
        const TTabletSnapshotPtr& tabletSnapshot) override
    {
        return New<TDictionaryCompressionFactory>(
            MakeWeak(tabletSnapshot),
            MakeWeak<ICompressionDictionaryManager>(this));
    }

    TFuture<TRowDictionaryCompressors> MaybeGetCompressors(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TClientChunkReadOptions& chunkReadOptions) override
    {
        std::vector<TFuture<TCompressionDictionaryCacheEntryPtr>> entryFutures;
        for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
            auto chunkId = tabletSnapshot->CompressionDictionaryInfos[policy].ChunkId;
            if (!chunkId) {
                continue;
            }

            auto key = TCompressionDictionaryCacheKey{
                .ChunkId = chunkId,
                .IsDecompression = false,
                .SchemaId = tabletSnapshot->SchemaId,
            };

            auto cookie = BeginInsert(key);
            entryFutures.push_back(cookie.GetValue());

            if (cookie.IsActive()) {
                PrepareDigestedDictionary(
                    tabletSnapshot,
                    chunkReadOptions,
                    std::move(cookie),
                    policy);
            }
        }

        if (entryFutures.empty()) {
            return {};
        }

        return AllSucceeded(std::move(entryFutures))
            .Apply(BIND([tabletSnapshot] (const std::vector<TCompressionDictionaryCacheEntryPtr>& entries) {
                const auto& schema = tabletSnapshot->PhysicalSchema;

                TRowDictionaryCompressors rowDictionaryCompressors;
                for (auto index = schema->GetKeyColumnCount(); index < schema->GetColumnCount(); ++index) {
                    if (schema->Columns()[index].MaxInlineHunkSize()) {
                        for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
                            EmplaceOrCrash(
                                rowDictionaryCompressors[policy].ColumnCompressors,
                                index,
                                TColumnDictionaryCompressor{});
                        }
                    }
                }

                for (const auto& entry : entries) {
                    const auto& rowDigestedDictionary = entry->GetRowDigestedCompressionDictionary();
                    auto& rowCompressor = rowDictionaryCompressors[entry->GetPolicy()];
                    rowCompressor.DictionaryId = entry->GetKey().ChunkId;
                    for (const auto& [valueId, columnDigestedDictionary] : rowDigestedDictionary) {
                        rowCompressor.ColumnCompressors[valueId] = TColumnDictionaryCompressor{
                            .Compressor = GetDictionaryCompressionCodec()->CreateDictionaryCompressor(
                                columnDigestedDictionary),
                        };
                    }
                }

                return rowDictionaryCompressors;
            }));
    }

    TFuture<THashMap<TChunkId, TRowDictionaryDecompressor>> GetDecompressors(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TClientChunkReadOptions& chunkReadOptions,
        const THashSet<TChunkId>& dictionaryIds) override
    {
        THashMap<TChunkId, TRowDictionaryDecompressor> result;
        std::vector<TFuture<TCompressionDictionaryCacheEntryPtr>> entryFutures;

        auto populateResult = [] (const auto& entry, auto* result) {
            TRowDictionaryDecompressor rowDecompressor;
            const auto& rowDigestedDictionary = entry->GetRowDigestedDecompressionDictionary();
            for (const auto& [valueId, columnDigestedDictionary] : rowDigestedDictionary) {
                EmplaceOrCrash(
                    rowDecompressor,
                    valueId,
                    GetDictionaryCompressionCodec()->CreateDictionaryDecompressor(columnDigestedDictionary));
            }

            EmplaceOrCrash(
                *result,
                entry->GetKey().ChunkId,
                std::move(rowDecompressor));
        };

        for (auto chunkId : dictionaryIds) {
            YT_VERIFY(chunkId);

            auto key = TCompressionDictionaryCacheKey{
                .ChunkId = chunkId,
                .IsDecompression = true,
                .SchemaId = tabletSnapshot->SchemaId,
            };

            if (const auto& entry = Find(key)) {
                populateResult(entry, &result);
            } else {
                auto cookie = BeginInsert(key);
                entryFutures.push_back(cookie.GetValue());

                if (cookie.IsActive()) {
                    PrepareDigestedDictionary(
                        tabletSnapshot,
                        chunkReadOptions,
                        std::move(cookie),
                        EDictionaryCompressionPolicy::None);
                }
            }
        }

        if (entryFutures.empty()) {
            YT_VERIFY(result.size() == dictionaryIds.size());
            return MakeFuture(std::move(result));
        }

        return AllSucceeded(std::move(entryFutures)).Apply(BIND(
            [
                populateResult = std::move(populateResult),
                result = std::move(result)
            ] (const std::vector<TCompressionDictionaryCacheEntryPtr>& entries) mutable {
                for (const auto& entry : entries) {
                    populateResult(entry, &result);
                }

                return std::move(result);
            }));
    }

private:
    using TCookie = TAsyncSlruCacheBase<
        TCompressionDictionaryCacheKey,
        TCompressionDictionaryCacheEntry>::TInsertCookie;

    IBootstrap* const Bootstrap_;

    // TODO(akozhikhov): Specific workload category?
    const EWorkloadCategory WorkloadCategory_ = EWorkloadCategory::SystemTabletCompaction;

    // TODO(akozhikhov): Memory usage tracker.

    i64 GetWeight(const TCompressionDictionaryCacheEntryPtr& entry) const override
    {
        return entry->GetMemoryUsage();
    }

    void PrepareDigestedDictionary(
        const TTabletSnapshotPtr& tabletSnapshot,
        const TClientChunkReadOptions& options,
        TCookie cookie,
        EDictionaryCompressionPolicy policy)
    {
        auto Logger = TabletNodeLogger.WithTag("%v",
            tabletSnapshot->LoggingTag);

        YT_LOG_DEBUG("Compression dictionary manager will read meta of a dictionary chunk (%v)",
            cookie.GetKey());

        TWallTimer metaWaitTimer;

        NChunkClient::NProto::TChunkSpec chunkSpec;
        ToProto(chunkSpec.mutable_chunk_id(), cookie.GetKey().ChunkId);

        auto chunkReaderHost = New<TChunkReaderHost>(
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalDescriptor(),
            /*blockCache*/ nullptr,
            /*chunkMetaCache*/ nullptr,
            Bootstrap_->GetHintManager(),
            Bootstrap_->GetInThrottler(WorkloadCategory_),
            Bootstrap_->GetReadRpsOutThrottler(),
            /*trafficMeter*/ nullptr);

        auto chunkReader = CreateRemoteReader(
            chunkSpec,
            // Using store reader config here to read hunk chunk meta seems fine.
            tabletSnapshot->Settings.StoreReaderConfig,
            New<TRemoteReaderOptions>(),
            std::move(chunkReaderHost));

        chunkReader->GetMeta(options)
            .Subscribe(BIND(
                &TCompressionDictionaryManager::OnDictionaryMetaRead,
                MakeStrong(this),
                chunkReader,
                tabletSnapshot,
                options,
                Passed(std::move(cookie)),
                policy,
                metaWaitTimer));
    }

    void OnDictionaryMetaRead(
        const IChunkReaderPtr& chunkReader,
        const TTabletSnapshotPtr& tabletSnapshot,
        TClientChunkReadOptions options,
        TCookie cookie,
        EDictionaryCompressionPolicy policy,
        TWallTimer metaWaitTimer,
        const TErrorOr<TRefCountedChunkMetaPtr>& metaOrError)
    {
        // Hold reader to prevent its destruction during meta read.
        Y_UNUSED(chunkReader);

        auto Logger = TabletNodeLogger.WithTag("%v",
            tabletSnapshot->LoggingTag);

        if (!metaOrError.IsOK()) {
            auto error = TError("Compression dictionary manager failed to read meta")
                << TErrorAttribute("key", cookie.GetKey())
                << metaOrError;
            YT_LOG_DEBUG(error);
            cookie.Cancel(error);
            return;
        }

        options.ChunkReaderStatistics->MetaWaitTime.fetch_add(
            metaWaitTimer.GetElapsedValue(),
            std::memory_order::relaxed);

        auto meta = metaOrError.Value();
        auto compressionDictionaryExt = GetProtoExtension<NTableClient::NProto::TCompressionDictionaryExt>(
            meta->extensions());

        std::vector<int> columnIdMapping;
        columnIdMapping.reserve(compressionDictionaryExt.column_infos_size());
        for (int index = 0; index < compressionDictionaryExt.column_infos_size(); ++index) {
            const auto& columnInfo = compressionDictionaryExt.column_infos(index);
            auto* column = tabletSnapshot->PhysicalSchema->FindColumnByStableName(FromProto<TColumnStableName>(
                columnInfo.stable_name()));
            YT_VERIFY(column);
            columnIdMapping.push_back(tabletSnapshot->PhysicalSchema->GetColumnIndex(*column));
        }

        auto erasureCodec = GetProtoExtension<NChunkClient::NProto::TMiscExt>(meta->extensions()).erasure_codec();
        YT_VERIFY(CheckedEnumCast<NErasure::ECodec>(erasureCodec) == NErasure::ECodec::None);

        std::vector<IChunkFragmentReader::TChunkFragmentRequest> requests;
        requests.reserve(compressionDictionaryExt.column_infos_size());
        for (int index = 0; index < compressionDictionaryExt.column_infos_size(); ++index) {
            const auto& columnInfo = compressionDictionaryExt.column_infos(index);
            requests.push_back({
                .ChunkId = cookie.GetKey().ChunkId,
                .Length = columnInfo.length(),
                .BlockIndex = columnInfo.block_index(),
                .BlockOffset = columnInfo.block_offset(),
            });
        }

        auto hunkChunkReaderStatistics = options.HunkChunkReaderStatistics;
        if (hunkChunkReaderStatistics) {
            options.ChunkReaderStatistics = hunkChunkReaderStatistics->GetChunkReaderStatistics();
        }

        YT_LOG_DEBUG("Compression dictionary manager will read fragments of a dictionary chunk "
            "(%v, FragmentCount: %v)",
            cookie.GetKey(),
            requests.size());

        tabletSnapshot->ChunkFragmentReader->ReadFragments(
            options,
            std::move(requests))
            .Subscribe(BIND(
                [=, cookie = std::move(cookie), columnIdMapping = std::move(columnIdMapping)]
                (const TErrorOr<IChunkFragmentReader::TReadFragmentsResponse>& responseOrError) mutable
            {
                if (!responseOrError.IsOK()) {
                    auto error = TError("Compression dictionary manager failed to read fragments")
                        << TErrorAttribute("key", cookie.GetKey())
                        << responseOrError;
                    YT_LOG_DEBUG(error);
                    cookie.Cancel(error);
                    return;
                }

                auto response = responseOrError.Value();
                YT_VERIFY(response.Fragments.size() == columnIdMapping.size());

                TCompressionDictionaryCacheEntryPtr entry;
                if (cookie.GetKey().IsDecompression) {
                    TRowDigestedDecompressionDictionary rowDigestedDictionary;
                    // NB: Columns that are missing in the dictionary will be ignored here
                    // and no decompression will be performed either.
                    for (int index = 0; index < std::ssize(columnIdMapping); ++index) {
                        YT_VERIFY(response.Fragments[index]);

                        auto digestedDictionary = GetDictionaryCompressionCodec()->CreateDigestedDecompressionDictionary(
                            response.Fragments[index]);
                        EmplaceOrCrash(
                            rowDigestedDictionary,
                            columnIdMapping[index],
                            std::move(digestedDictionary));
                    }
                    entry = New<TCompressionDictionaryCacheEntry>(
                        cookie.GetKey(),
                        std::move(rowDigestedDictionary),
                        policy);
                } else {
                    TRowDigestedCompressionDictionary rowDigestedDictionary;
                    // NB: Columns that are missing in the dictionary will be ignored here
                    // and no compression will be performed either.
                    for (int index = 0; index < std::ssize(columnIdMapping); ++index) {
                        YT_VERIFY(response.Fragments[index]);

                        auto digestedDictionary = GetDictionaryCompressionCodec()->CreateDigestedCompressionDictionary(
                            response.Fragments[index],
                            tabletSnapshot->Settings.MountConfig->ValueDictionaryCompression->CompressionLevel);
                        EmplaceOrCrash(
                            rowDigestedDictionary,
                            columnIdMapping[index],
                            std::move(digestedDictionary));
                    }
                    entry = New<TCompressionDictionaryCacheEntry>(
                        cookie.GetKey(),
                        std::move(rowDigestedDictionary),
                        policy);
                }

                YT_LOG_DEBUG("Compression dictionary manager successfully read fragments of a dictionary chunk "
                    "(%v, MemoryUsage: %v)",
                    cookie.GetKey(),
                    entry->GetMemoryUsage());

                cookie.EndInsert(std::move(entry));

                if (hunkChunkReaderStatistics) {
                    hunkChunkReaderStatistics->DataWeight() += GetByteSize(response.Fragments);
                    hunkChunkReaderStatistics->RefValueCount() += std::ssize(response.Fragments);
                    hunkChunkReaderStatistics->BackendReadRequestCount() += response.BackendReadRequestCount;
                    hunkChunkReaderStatistics->BackendHedgingReadRequestCount() += response.BackendHedgingReadRequestCount;
                    hunkChunkReaderStatistics->BackendProbingRequestCount() += response.BackendProbingRequestCount;
                }
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

ICompressionDictionaryManagerPtr CreateCompressionDictionaryManager(
    TSlruCacheConfigPtr config,
    IBootstrap* bootstrap)
{
    return New<TCompressionDictionaryManager>(
        std::move(config),
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
