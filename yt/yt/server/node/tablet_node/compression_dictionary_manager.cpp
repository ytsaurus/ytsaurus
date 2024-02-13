#include "compression_dictionary_manager.h"

#include "tablet.h"

#include <yt/yt/server/lib/tablet_node/config.h>

#include <yt/yt/core/compression/dictionary_codec.h>

#include <library/cpp/yt/logging/logger.h>

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

    const NLogging::TLogger Logger;

    TRowDictionaryCompressors RowDictionaryCompressors_;

    std::optional<EDictionaryCompressionPolicy> ElectedPolicy_;
    i64 ProcessedSamplesSize_ = 0;
    int ProcessedSampleCount_ = 0;

    TDuration CompressionTime_;


    void ElectBestPolicy()
    {
        YT_VERIFY(!ElectedPolicy_);

        TEnumIndexedVector<EDictionaryCompressionPolicy, i64> policyToCompressedSize;
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

        if (IsStringLikeType(value.Type)) {
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
            Promise_.TrySet(TError(NYT::EErrorCode::Canceled, "Reader was destroyed"));
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

        auto decompressors = std::move(decompressorsOrError.Value());

        auto* codec = GetDictionaryCompressionCodec();

        TWallTimer decompressionTimer;

        i64 decompressedSize = 0;
        std::vector<i64> contentSizes;
        auto newDecompressedValueCount = decompressedValueCount;
        while (newDecompressedValueCount < std::ssize(values)) {
            auto dictionaryId = dictionaryIds[newDecompressedValueCount];
            YT_VERIFY(dictionaryId != NullChunkId);

            auto* compressedValue = values[newDecompressedValueCount];
            YT_ASSERT(IsStringLikeType(compressedValue->Type));
            YT_VERIFY(None(compressedValue->Flags & EValueFlags::Hunk));

            auto frameInfo = codec->GetFrameInfo(TRef(compressedValue->Data.String, compressedValue->Length));
            contentSizes.push_back(frameInfo.ContentSize);
            decompressedSize += contentSizes.back();

            ++newDecompressedValueCount;

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
        for (int valueIndex = decompressedValueCount; valueIndex < newDecompressedValueCount; ++valueIndex) {
            auto dictionaryId = dictionaryIds[valueIndex];
            YT_VERIFY(dictionaryId != NullChunkId);

            auto* compressedValue = values[valueIndex];
            TRef compressedValueRef(compressedValue->Data.String, compressedValue->Length);

            const auto& rowDecompressor = GetOrCrash(decompressors, dictionaryId);
            auto columnDecompressorIt = rowDecompressor.find(compressedValue->Id);
            if (columnDecompressorIt == rowDecompressor.end()) {
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
            "(DecompressionTime: %v, RowCount: %v/%v, CompressedSize: %v, DecompressedSize: %v)",
            decompressionTimer.GetElapsedTime(),
            newDecompressedValueCount,
            values.size(),
            currentCompressedSize,
            currentDecompressedSize);

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
    IDictionaryCompressionSessionPtr CreateDictionaryCompressionSession() const override
    {
        TRowDictionaryCompressors compressors;
        return New<TDictionaryCompressionSession>(
            TabletSnapshot_,
            compressors);
    }

    IDictionaryDecompressionSessionPtr CreateDictionaryDecompressionSession() const override
    {
        return New<TDictionaryDecompressionSession>(
            TabletSnapshot_,
            DictionaryManager_);
    }

private:
    const TTabletSnapshotPtr TabletSnapshot_;
    const TWeakPtr<ICompressionDictionaryManager> DictionaryManager_ = {};
};

////////////////////////////////////////////////////////////////////////////////

class TCompressionDictionaryManager
    : public ICompressionDictionaryManager
{
public:
    IDictionaryCompressionFactoryPtr CreateTabletDictionaryCompressionFactory() const override
    {
        return New<TDictionaryCompressionFactory>();
    }

    TFuture<THashMap<TChunkId, TRowDictionaryDecompressor>> GetDecompressors(
        const TTabletSnapshotPtr& /*tabletSnapshot*/,
        const TClientChunkReadOptions& /*chunkReadOptions*/,
        const THashSet<TChunkId>& /*dictionaryIds*/) override
    {
        YT_UNIMPLEMENTED();
    }
};

////////////////////////////////////////////////////////////////////////////////

ICompressionDictionaryManagerPtr CreateCompressionDictionaryManager()
{
    return New<TCompressionDictionaryManager>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
