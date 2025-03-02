#include "compression_dictionary_manager.h"

#include "bootstrap.h"
#include "tablet.h"
#include "hint_manager.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>

#include <yt/yt/ytlib/table_client/chunk_meta_extensions.h>

#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/compression/dictionary_codec.h>

#include <yt/yt/core/misc/async_slru_cache.h>

#include <library/cpp/yt/logging/logger.h>

#include <util/random/random.h>

namespace NYT::NTabletNode {

using namespace NChunkClient;
using namespace NCompression;
using namespace NProfiling;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDictionaryCompressionSession)

class TDictionaryCompressionSession
    : public IDictionaryCompressionSession
{
public:
    TDictionaryCompressionSession(
        const TTabletSnapshotPtr& tabletSnapshot,
        TRowDictionaryCompressors rowDictionaryCompressors,
        std::optional<EDictionaryCompressionPolicy> presetPolicy)
        : Schema_(tabletSnapshot->PhysicalSchema)
        , ProbationSize_(tabletSnapshot->Settings.MountConfig
            ->ValueDictionaryCompression->PolicyProbationSamplesSize)
        , MaxAcceptableCompressionRatio_(tabletSnapshot->Settings.MountConfig
            ->ValueDictionaryCompression->MaxAcceptableCompressionRatio)
        , ElectRandomPolicy_(tabletSnapshot->Settings.MountConfig
            ->ValueDictionaryCompression->ElectRandomPolicy)
        , Logger(TabletNodeLogger().WithTag("%v",
            tabletSnapshot->LoggingTag))
        , RowDictionaryCompressors_(std::move(rowDictionaryCompressors))
    {
        if (presetPolicy) {
            ElectedPolicy_ = presetPolicy;
            YT_LOG_DEBUG("Dictionary compression session elected policy is predefined (Policy: %v, ChunkId: %v)",
                ElectedPolicy_,
                RowDictionaryCompressors_[*ElectedPolicy_].DictionaryId);
        }
    }

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
            const auto& columnDecompressor = GetOrCrash(
                RowDictionaryCompressors_[*ElectedPolicy_].ColumnDecompressors,
                value.Id);
            if (!columnCompressor.Compressor) {
                continue;
            }

            TRef initialValue(value.Data.String, value.Length);
            auto compressedValue = columnCompressor.Compressor->Compress(
                pool,
                initialValue);

            YT_VERIFY(columnDecompressor);
            char* output = pool->AllocateUnaligned(value.Length);
            TMutableRef decompressedValue(output, value.Length);
            columnDecompressor->Decompress(compressedValue, decompressedValue);
            if (!TRef::AreBitwiseEqual(initialValue, decompressedValue)) {
                auto error = TError("Value decompression double-check failed")
                    << TErrorAttribute("policy", *ElectedPolicy_)
                    << TErrorAttribute("dictonary_id", RowDictionaryCompressors_[*ElectedPolicy_].DictionaryId)
                    << TErrorAttribute("value_id", value.Id)
                    << TErrorAttribute("row", ToString(*row))
                    << TErrorAttribute("value", ToString(value));
                YT_LOG_ALERT(error);
                THROW_ERROR(error);
            }
            pool->Free(output, output + value.Length);

            value.Data.String = compressedValue.Begin();
            value.Length = compressedValue.Size();
        }

        CompressionTime_ += compressionTimer.GetElapsedTime();
    }

    TChunkId GetCompressionDictionaryId() const override
    {
        if (!ElectedPolicy_) {
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
    const bool ElectRandomPolicy_;

    const NLogging::TLogger Logger;

    TRowDictionaryCompressors RowDictionaryCompressors_;

    std::optional<EDictionaryCompressionPolicy> ElectedPolicy_;
    i64 ProcessedSamplesSize_ = 0;
    int ProcessedSampleCount_ = 0;

    TDuration CompressionTime_;


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

        // Solely for testing purposes.
        if (ElectRandomPolicy_) {
            auto randomIndex = RandomNumber<ui32>(TEnumTraits<EDictionaryCompressionPolicy>::GetDomainSize());
            ElectedPolicy_ = TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()[randomIndex];
        }

        YT_LOG_DEBUG("Dictionary compression session elected best policy "
            "(Policy: %v, ChunkId: %v, ProcessedSampleCount: %v, ProcessedSamplesSize: %v, "
            "PolicyToCompressedSize: %v, ElectRandomPolicy: %v)",
            ElectedPolicy_,
            RowDictionaryCompressors_[*ElectedPolicy_].DictionaryId,
            ProcessedSampleCount_,
            ProcessedSamplesSize_,
            policyToCompressedSize,
            ElectRandomPolicy_);
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

        if (value.Length == 0) {
            return false;
        }

        return true;
    }
};

DEFINE_REFCOUNTED_TYPE(TDictionaryCompressionSession)

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
        const TClientChunkReadOptions& chunkReadOptions,
        std::optional<TChunkId> presetCompressionDictionaryId) const override
    {
        auto dictionaryManager = DictionaryManager_.Lock();
        auto tabletSnapshot = TabletSnapshot_.Lock();
        if (!dictionaryManager || !tabletSnapshot) {
            auto error = TError(NYT::EErrorCode::Canceled,
                "Unable to compress values due to tablet cell reconfiguration");
            return MakeFuture<IDictionaryCompressionSessionPtr>(error);
        }

        // Preset null dictionary id means that compression shall be disabled.
        if (presetCompressionDictionaryId && !*presetCompressionDictionaryId) {
            return TFuture<IDictionaryCompressionSessionPtr>{};
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
                presetCompressionDictionaryId,
                tabletSnapshot = std::move(tabletSnapshot)
            ] (TRowDictionaryCompressors&& compressors) {
                std::optional<EDictionaryCompressionPolicy> presetPolicy;
                if (presetCompressionDictionaryId) {
                    for (auto policy : TEnumTraits<EDictionaryCompressionPolicy>::GetDomainValues()) {
                        if (presetCompressionDictionaryId == compressors[policy].DictionaryId) {
                            YT_VERIFY(!presetPolicy);
                            presetPolicy = policy;
                        }
                    }
                    YT_VERIFY(presetPolicy);
                }

                IDictionaryCompressionSessionPtr session = New<TDictionaryCompressionSession>(
                    tabletSnapshot,
                    std::move(compressors),
                    presetPolicy);
                return session;
            }));
    }

    IDictionaryDecompressionSessionPtr CreateDictionaryDecompressionSession() override
    {
        auto tabletSnapshot = TabletSnapshot_.Lock();
        if (!tabletSnapshot) {
            THROW_ERROR_EXCEPTION(NYT::EErrorCode::Canceled,
                "Unable to decompress values due to tablet cell reconfiguration");
        }

        return NTableClient::CreateDictionaryDecompressionSession(
            MakeWeak(this),
            tabletSnapshot->Settings.HunkReaderConfig,
            TabletNodeLogger().WithTag("%v",
                tabletSnapshot->LoggingTag));
    }

    TFuture<THashMap<TChunkId, TRowDictionaryDecompressor>> GetDecompressors(
        const TClientChunkReadOptions& chunkReadOptions,
        const THashSet<TChunkId>& dictionaryIds) override
    {
        auto dictionaryManager = DictionaryManager_.Lock();
        auto tabletSnapshot = TabletSnapshot_.Lock();
        if (!dictionaryManager || !tabletSnapshot) {
            auto error = TError(NYT::EErrorCode::Canceled,
                "Unable to get decompressors due to tablet cell reconfiguration");
            return MakeFuture<THashMap<TChunkId, TRowDictionaryDecompressor>>(error);
        }

        return dictionaryManager->GetDecompressors(
            tabletSnapshot,
            chunkReadOptions,
            dictionaryIds);
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
        if (GetKey().IsDecompression) {
            return std::get<1>(RowDictionary_).StorageSize;
        } else {
            return std::get<0>(RowDictionary_).StorageSize;
        }
    }

    i64 GetEffectiveMemoryUsage() const
    {
        i64 memoryUsage = 0;
        if (GetKey().IsDecompression) {
            for (const auto& [_, digestedDictionary] : std::get<1>(RowDictionary_).ColumnDictionaries) {
                memoryUsage += digestedDictionary->GetMemoryUsage();
            }
        } else {
            for (const auto& [_, digestedDictionary] : std::get<0>(RowDictionary_).ColumnDictionaries) {
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
            TabletNodeProfiler().WithPrefix("/compression_dictionary_cache"))
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

            auto getEntryFuture = [&] (bool isDecompression) {
                auto key = TCompressionDictionaryCacheKey{
                    .ChunkId = chunkId,
                    .IsDecompression = isDecompression,
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
            };

            getEntryFuture(/*isDecompression*/ false);
            getEntryFuture(/*isDecompression*/ true);
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
                            EmplaceOrCrash(
                                rowDictionaryCompressors[policy].ColumnDecompressors,
                                index,
                                nullptr);
                        }
                    }
                }

                int entryIndex = 0;
                YT_VERIFY(entries.size() % 2 == 0);
                while (entryIndex < std::ssize(entries)) {
                    const auto& compressionEntry = entries[entryIndex];
                    const auto& decompressionEntry = entries[entryIndex + 1];
                    YT_VERIFY(compressionEntry->GetKey().ChunkId == decompressionEntry->GetKey().ChunkId);
                    entryIndex += 2;

                    const auto& compressionDictionary = compressionEntry->GetRowDigestedCompressionDictionary();
                    const auto& decompressionDictionary = decompressionEntry->GetRowDigestedDecompressionDictionary();
                    YT_VERIFY(compressionDictionary.ColumnDictionaries.size() ==
                        decompressionDictionary.ColumnDictionaries.size());

                    auto& rowCompressor = rowDictionaryCompressors[compressionEntry->GetPolicy()];
                    rowCompressor.DictionaryId = compressionEntry->GetKey().ChunkId;
                    for (const auto& [valueId, columnDigestedCompressionDictionary] : compressionDictionary.ColumnDictionaries) {
                        rowCompressor.ColumnCompressors[valueId] = TColumnDictionaryCompressor{
                            .Compressor = GetDictionaryCompressionCodec()->CreateDictionaryCompressor(
                                columnDigestedCompressionDictionary),
                        };

                        const auto& columnDigestedDecompressionDictionary = GetOrCrash(
                            decompressionDictionary.ColumnDictionaries,
                            valueId);
                        rowCompressor.ColumnDecompressors[valueId] = GetDictionaryCompressionCodec()->
                            CreateDictionaryDecompressor(columnDigestedDecompressionDictionary);
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
            EmplaceOrCrash(
                *result,
                entry->GetKey().ChunkId,
                CreateRowDictionaryDecompressor(entry->GetRowDigestedDecompressionDictionary()));
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
        const TClientChunkReadOptions& chunkReadOptions,
        TCookie cookie,
        EDictionaryCompressionPolicy policy)
    {
        auto Logger = TabletNodeLogger().WithTag("%v",
            tabletSnapshot->LoggingTag);

        auto chunkReaderHost = New<TChunkReaderHost>(
            Bootstrap_->GetClient(),
            Bootstrap_->GetLocalDescriptor(),
            /*blockCache*/ nullptr,
            /*chunkMetaCache*/ nullptr,
            Bootstrap_->GetHintManager(),
            Bootstrap_->GetInThrottler(WorkloadCategory_),
            Bootstrap_->GetReadRpsOutThrottler(),
            NConcurrency::GetUnlimitedThrottler(),
            /*trafficMeter*/ nullptr);

        ReadDigestedDictionary(
            cookie.GetKey().ChunkId,
            cookie.GetKey().IsDecompression,
            std::move(chunkReaderHost),
            tabletSnapshot->Settings.StoreReaderConfig,
            tabletSnapshot->Settings.HunkReaderConfig,
            chunkReadOptions,
            TNameTable::FromSchemaStable(*tabletSnapshot->PhysicalSchema),
            tabletSnapshot->ChunkFragmentReader,
            Logger)
            .SubscribeUnique(BIND(
                [=, cookie = std::move(cookie)] (TErrorOr<TRowDigestedDictionary>&& digestedDictionaryOrError) mutable
            {
                if (!digestedDictionaryOrError.IsOK()) {
                    auto error = TError("Compression dictionary manager failed to read digested dictionary")
                        << digestedDictionaryOrError;
                    YT_LOG_DEBUG(error);
                    cookie.Cancel(error);
                    return;
                };

                auto entry = New<TCompressionDictionaryCacheEntry>(
                    cookie.GetKey(),
                    std::move(digestedDictionaryOrError.Value()),
                    policy);

                YT_LOG_DEBUG("Compression dictionary manager successfully read digested dictionary "
                    "(%v, MemoryUsage: %v, EffectiveMemoryUsage: %v)",
                    cookie.GetKey(),
                    entry->GetMemoryUsage(),
                    entry->GetEffectiveMemoryUsage());

                cookie.EndInsert(std::move(entry));
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
