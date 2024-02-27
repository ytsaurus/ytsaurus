#include "journal_hunk_chunk_writer.h"

#include "config.h"
#include "journal_chunk_writer.h"
#include "helpers.h"

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/library/erasure/impl/codec.h>

namespace NYT::NJournalClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TJournalHunkChunkWriter
    : public IJournalHunkChunkWriter
{
public:
    TJournalHunkChunkWriter(
        NApi::NNative::IClientPtr client,
        TSessionId sessionId,
        TJournalHunkChunkWriterOptionsPtr options,
        TJournalHunkChunkWriterConfigPtr config,
        const NLogging::TLogger& logger)
        : UnderlyingWriter_(CreateJournalChunkWriter(
            std::move(client),
            sessionId,
            options,
            PrepareJournalChunkWriterConfig(config),
            logger))
        , Options_(std::move(options))
        , Config_(std::move(config))
        , Logger(logger.WithTag("ChunkId: %v", sessionId.ChunkId))
        , ChunkId_(sessionId.ChunkId)
    {
        auto guard = Guard(Lock_);
        ScheduleCurrentRecordFlush();
    }

    TFuture<void> Open() override
    {
        return UnderlyingWriter_->Open();
    }

    TFuture<void> Close() override
    {
        return UnderlyingWriter_->Close();
    }

    TFuture<std::vector<TJournalHunkDescriptor>> WriteHunks(std::vector<TSharedRef> payloads) override
    {
        YT_VERIFY(!payloads.empty());

        std::vector<TFuture<std::vector<TJournalHunkDescriptor>>> futures;

        bool newRecord = true;
        std::optional<TGuard<NThreading::TSpinLock>> guard;
        for (auto& payload : payloads) {
            if (newRecord) {
                newRecord = false;
                guard.emplace(Guard(Lock_));
                PromiseIndexToPayloadCount_.push_back(0);
                CurrentRecordPromises_.push_back(NewPromise<std::vector<TJournalHunkDescriptor>>());
                futures.push_back(CurrentRecordPromises_.back().ToFuture());
            }

            auto hunkSize = GetHunkSize(payload);

            ++Statistics_.HunkCount;
            Statistics_.TotalSize += hunkSize;

            CurrentRecordSize_ += hunkSize;
            CurrentRecordPayloads_.push_back(std::move(payload));
            ++PromiseIndexToPayloadCount_.back();

            // NB: May release guard.
            if (FlushIfNeeded(guard)) {
                newRecord = true;
            }
        }

        YT_VERIFY(!futures.empty());
        if (futures.size() == 1) {
            return futures[0];
        } else {
            return AllSucceeded(std::move(futures))
                .ApplyUnique(BIND([
                    payloadCount = std::ssize(payloads)
                ] (std::vector<std::vector<TJournalHunkDescriptor>>&& descriptorLists) {
                    std::vector<TJournalHunkDescriptor> result;
                    result.reserve(payloadCount);
                    for (auto& descriptors : descriptorLists) {
                        std::move(descriptors.begin(), descriptors.end(), std::back_inserter(result));
                    }
                    return result;
                }));
        }
    }

    TJournalHunkChunkWriterStatistics GetStatistics() const override
    {
        auto guard = Guard(Lock_);

        return Statistics_;
    }

    bool IsCloseDemanded() const override
    {
        return UnderlyingWriter_->IsCloseDemanded();
    }

private:
    const IJournalChunkWriterPtr UnderlyingWriter_;

    const TJournalHunkChunkWriterOptionsPtr Options_;
    const TJournalHunkChunkWriterConfigPtr Config_;

    const NLogging::TLogger Logger;

    const TChunkId ChunkId_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    std::vector<TSharedRef> CurrentRecordPayloads_;
    std::vector<TPromise<std::vector<TJournalHunkDescriptor>>> CurrentRecordPromises_;
    std::vector<int> PromiseIndexToPayloadCount_;

    i64 CurrentRecordSize_ = 0;
    i64 CurrentRecordIndex_ = 0;

    TDelayedExecutorCookie CurrentRecordFlushCookie_;

    TJournalHunkChunkWriterStatistics Statistics_;

    void ScheduleCurrentRecordFlush()
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        CurrentRecordFlushCookie_ = TDelayedExecutor::Submit(
            BIND(&TJournalHunkChunkWriter::OnRecordFlushTimeout, MakeWeak(this), CurrentRecordIndex_),
            Config_->MaxBatchDelay);
    }

    void OnRecordFlushTimeout(i64 recordIndex)
    {
        auto guard = std::make_optional(Guard(Lock_));

        if (CurrentRecordIndex_ != recordIndex) {
            return;
        }

        if (CurrentRecordPayloads_.empty()) {
            ScheduleCurrentRecordFlush();
            return;
        }

        // NB: Releases guard.
        FlushCurrentRecord(guard);
    }

    bool FlushIfNeeded(std::optional<TGuard<NThreading::TSpinLock>>& guard)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);

        if (CurrentRecordSize_ >= Config_->MaxRecordSize ||
            std::ssize(CurrentRecordPayloads_) >= Config_->MaxRecordHunkCount)
        {
            FlushCurrentRecord(guard);
            return true;
        } else {
            return false;
        }
    }

    void FlushCurrentRecord(std::optional<TGuard<NThreading::TSpinLock>>& guard)
    {
        VERIFY_SPINLOCK_AFFINITY(Lock_);
        YT_VERIFY(!CurrentRecordPayloads_.empty());

        YT_LOG_DEBUG("Flushing journal hunk chunk record "
            "(RecordIndex: %v, RecordSize: %v, RecordHunkCount: %v, FutureCount: %v)",
            CurrentRecordIndex_,
            CurrentRecordSize_,
            std::ssize(CurrentRecordPayloads_),
            CurrentRecordPromises_.size());

        auto recordFlushFuture = Options_->ErasureCodec == NErasure::ECodec::None
            ? FlushRegularRecord()
            : FlushErasureRecord();

        auto currentRecordPayloads = std::exchange(CurrentRecordPayloads_, {});
        auto currentRecordPromises = std::exchange(CurrentRecordPromises_, {});
        auto promiseIndexToPayloadCount = std::exchange(PromiseIndexToPayloadCount_, {});
        auto currentRecordIndex = CurrentRecordIndex_++;
        auto currentRecordSize = std::exchange(CurrentRecordSize_, 0);

        TDelayedExecutor::CancelAndClear(CurrentRecordFlushCookie_);

        ScheduleCurrentRecordFlush();

        guard.reset();

        YT_VERIFY(currentRecordPromises.size() == promiseIndexToPayloadCount.size());
        i64 globalPayloadIndex = 0;
        i64 currentRecordOffset = 0;

        std::vector<std::vector<TJournalHunkDescriptor>> descriptorLists;
        descriptorLists.reserve(currentRecordPromises.size());
        for (auto payloadCount : promiseIndexToPayloadCount) {
            auto& descriptors = descriptorLists.emplace_back();
            descriptors.reserve(payloadCount);
            for (int localPayloadIndex = 0; localPayloadIndex < payloadCount; ++localPayloadIndex) {
                auto hunkSize = GetHunkSize(currentRecordPayloads[globalPayloadIndex]);
                descriptors.push_back({
                    .ChunkId = ChunkId_,
                    .RecordIndex = currentRecordIndex,
                    .RecordOffset = currentRecordOffset,
                    .Length = hunkSize,
                    .ErasureCodec = Options_->ErasureCodec,
                    .RecordSize = currentRecordSize,
                });

                currentRecordOffset += hunkSize;
                ++globalPayloadIndex;
            }
        }

        YT_VERIFY(globalPayloadIndex == std::ssize(currentRecordPayloads));
        YT_VERIFY(currentRecordOffset == currentRecordSize);

        recordFlushFuture.Subscribe(BIND([
            promises = std::move(currentRecordPromises),
            descriptorLists = std::move(descriptorLists)
        ] (const TError& error) mutable {
            YT_VERIFY(promises.size() == descriptorLists.size());

            for (int promiseIndex = 0; promiseIndex < std::ssize(promises); ++promiseIndex) {
                if (error.IsOK()) {
                    promises[promiseIndex].TrySet(std::move(descriptorLists[promiseIndex]));
                } else{
                    promises[promiseIndex].TrySet(error);
                }
            }
        }));
    }

    TFuture<void> FlushRegularRecord()
    {
        struct TJournalHunkChunkWriterTag
        { };
        auto record = TSharedMutableRef::Allocate<TJournalHunkChunkWriterTag>(
            CurrentRecordSize_,
            TSharedMutableRefAllocateOptions{
                .InitializeStorage = false,
            });

        auto* ptr = record.Begin();
        for (const auto& payload : CurrentRecordPayloads_) {
            // Write header.
            auto* header = reinterpret_cast<THunkPayloadHeader*>(ptr);
            header->Checksum = GetChecksum(payload);
            ptr += sizeof(THunkPayloadHeader);

            // Write payload.
            WriteRef(ptr, TRef(payload.Begin(), payload.Size()));
        }
        YT_VERIFY(ptr == record.End());

        return UnderlyingWriter_->WriteRecord(std::move(record));
    }

    TFuture<void> FlushErasureRecord()
    {
        auto* codec = NErasure::GetCodec(Options_->ErasureCodec);
        auto dataPartCount = codec->GetDataPartCount();
        auto totalPartCount = codec->GetTotalPartCount();

        // NB: We prepend TErasureRowHeader for compatibility only as chunk fragment reader
        // does not distinguish non-hunk and hunk erasure journal chunks.
        auto partSize = DivCeil<i64>(CurrentRecordSize_ + sizeof(TErasureRowHeader), dataPartCount);

        struct TJournalErasureHunkChunkWriterTag
        { };
        auto record = TSharedMutableRef::Allocate<TJournalErasureHunkChunkWriterTag>(
            partSize * dataPartCount,
            TSharedMutableRefAllocateOptions{
                .InitializeStorage = false,
            });

        char* ptr = record.Begin();
        auto* rowHeader = reinterpret_cast<TErasureRowHeader*>(ptr);
        rowHeader->PaddingSize = 0;
        ptr += sizeof(TErasureRowHeader);
        for (const auto& payload : CurrentRecordPayloads_) {
            // Write header.
            auto* payloadHeader = reinterpret_cast<THunkPayloadHeader*>(ptr);
            payloadHeader->Checksum = GetChecksum(payload);
            ptr += sizeof(THunkPayloadHeader);

            // Write payload.
            WriteRef(ptr, TRef(payload.Begin(), payload.Size()));
        }
        // FIXME(akozhikhov): This is not serialization aligned now (as well as in other erasure codecs probably).
        WriteZeroes(ptr, partSize * dataPartCount - CurrentRecordSize_ - sizeof(TErasureRowHeader));
        YT_VERIFY(ptr == record.End());

        std::vector<TSharedRef> parts;
        parts.reserve(totalPartCount);
        for (int index = 0; index < dataPartCount; ++index) {
            parts.push_back(record.Slice(index * partSize, (index + 1) * partSize));
        }

        auto parityParts = codec->Encode(parts);
        for (auto& parityPart : parityParts) {
            parts.push_back(std::move(parityPart));
        }

        return UnderlyingWriter_->WriteEncodedRecordParts(std::move(parts));
    }

    static i64 GetHunkSize(const TSharedRef& payload)
    {
        return sizeof(THunkPayloadHeader) + payload.Size();
    }

    static TJournalChunkWriterConfigPtr PrepareJournalChunkWriterConfig(
        const TJournalHunkChunkWriterConfigPtr& config)
    {
        // Do not batch records in journal chunk writer.
        auto newConfig = CloneYsonStruct(config);
        newConfig->MaxBatchDelay = TDuration::Zero();
        newConfig->MaxBatchRowCount = 1;

        return newConfig;
    }
};

////////////////////////////////////////////////////////////////////////////////

IJournalHunkChunkWriterPtr CreateJournalHunkChunkWriter(
    NApi::NNative::IClientPtr client,
    TSessionId sessionId,
    TJournalHunkChunkWriterOptionsPtr options,
    TJournalHunkChunkWriterConfigPtr config,
    const NLogging::TLogger& logger)
{
    return New<TJournalHunkChunkWriter>(
        std::move(client),
        sessionId,
        std::move(options),
        std::move(config),
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
