#include "file_changelog_index.h"
#include "format.h"
#include "config.h"
#include "private.h"

#include <yt/yt/server/lib/io/io_engine.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/checksum.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

namespace NYT::NHydra {

using namespace NConcurrency;
using namespace NIO;

////////////////////////////////////////////////////////////////////////////////

struct TFileChangelogIndexTag
{ };

TFileChangelogIndex::TFileChangelogIndex(
    IIOEnginePtr ioEngine,
    TString fileName,
    TFileChangelogConfigPtr config)
    : IOEngine_(std::move(ioEngine))
    , FileName_(std::move(fileName))
    , Config_(std::move(config))
    , Logger(HydraLogger.WithTag("Path: %v", FileName_))
{ }

EFileChangelogIndexOpenResult TFileChangelogIndex::Open()
{
    Clear();

    if (!NFS::Exists(FileName_)) {
        Create();
        return EFileChangelogIndexOpenResult::MissingCreated;
    }

    auto handle = WaitFor(IOEngine_->Open({.Path = FileName_, .Mode = RdWr}))
        .ValueOrThrow();

    auto recreate = [&] (EFileChangelogIndexOpenResult result) {
        if (handle) {
            WaitFor(IOEngine_->Close({.Handle = handle}))
                .ThrowOnError();
        }

        Create();
        return result;
    };

    auto buffer = WaitFor(
        IOEngine_->Read(
            {{.Handle = handle, .Offset = 0, .Size = handle->GetLength()}},
            // TODO(babenko): better workload category?
            EWorkloadCategory::UserBatch,
            GetRefCountedTypeCookie<TFileChangelogIndexTag>()))
        .ValueOrThrow()
        .OutputBuffers[0];

    const auto* current = buffer.Begin();
    const auto* end = buffer.End();

    if (current + sizeof(TChangelogIndexHeader) > end) {
        YT_LOG_WARNING("Changelog index file is too short to fit header, recreating (Length: %v)",
            buffer.Size());
        return recreate(EFileChangelogIndexOpenResult::ExistingRecreatedBrokenIndexHeader);
    }

    const auto* header = reinterpret_cast<const TChangelogIndexHeader*>(current);
    if (header->Signature != TChangelogIndexHeader::ExpectedSignature) {
        YT_LOG_WARNING("Changelog index file signature mismatch, recreating (ExpectedSignature: %llx, ActualSignature: %llx)",
            TChangelogIndexHeader::ExpectedSignature,
            header->Signature);
        return recreate(EFileChangelogIndexOpenResult::ExistingRecreatedBrokenIndexHeader);
    }

    current += sizeof(TChangelogIndexHeader);

    auto truncate = [&] (EFileChangelogIndexOpenResult result) {
        IndexFilePosition_ = current - buffer.Begin();

        WaitFor(IOEngine_->Resize({.Handle = handle, .Size = IndexFilePosition_}))
            .ThrowOnError();

        WaitFor(IOEngine_->FlushFile({.Handle = handle, .Mode = EFlushFileMode::All}))
            .ThrowOnError();

        YT_LOG_DEBUG("Changelog index file truncated (RecordCount: %v, IndexFilePosition: %v, DataFileLength: %v)",
            RecordOffsets_.size(),
            IndexFilePosition_,
            DataFileLength_);

        Handle_ = std::move(handle);
        return result;
    };

    auto finish = [&] {
        IndexFilePosition_ = current - buffer.Begin();

        YT_LOG_DEBUG("Changelog index file opened (RecordCount: %v, IndexFilePosition: %v, DataFileLength: %v)",
            RecordOffsets_.size(),
            IndexFilePosition_,
            DataFileLength_);

        Handle_ = std::move(handle);
        return EFileChangelogIndexOpenResult::ExistingOpened;
    };

    while (current != end) {
        if (current + sizeof(TChangelogIndexSegmentHeader) > end) {
            YT_LOG_WARNING("Changelog index file is too short to fit segment header, truncating (IndexFilePosition: %v, Length: %v)",
                current - buffer.Begin(),
                buffer.Size());
            return truncate(EFileChangelogIndexOpenResult::ExistingTruncatedBrokenSegment);
        }

        const auto* segmentHeader = reinterpret_cast<const TChangelogIndexSegmentHeader*>(current);
        if (segmentHeader->RecordCount < 0) {
            YT_LOG_ALERT("Negative number of records in changelog index segment, recreating (IndexFilePosition: %v)",
                current - buffer.Begin());
            return recreate(EFileChangelogIndexOpenResult::ExistingRecreatedBrokenSegmentHeader);
        }

        const auto* segmentEnd = current + sizeof(TChangelogIndexSegmentHeader) + sizeof(TChangelogIndexRecord) * segmentHeader->RecordCount;
        if (segmentEnd > end) {
            YT_LOG_WARNING("Changelog index file is too short to fit full segment, truncating (IndexFilePosition: %v, Length: %v, RecordCount: %v)",
                current - buffer.Begin(),
                buffer.Size(),
                segmentHeader->RecordCount);
            return truncate(EFileChangelogIndexOpenResult::ExistingTruncatedBrokenSegment);
        }

        if (GetChecksum(buffer.Slice(current + sizeof(ui64), segmentEnd)) != segmentHeader->Checksum) {
            YT_LOG_ALERT("Invalid changelog index segment checksum, recreating (IndexFilePosition: %v)",
                current - buffer.Begin());
            return recreate(EFileChangelogIndexOpenResult::ExistingRecreatedBrokenSegmentChecksum);
        }

        current += sizeof(TChangelogIndexSegmentHeader);
        while (current != segmentEnd) {
            const auto* record = reinterpret_cast<const TChangelogIndexRecord*>(current);
            YT_VERIFY(DataFileLength_ < 0 || DataFileLength_ == record->Offset);
            YT_VERIFY(record->Length >= 0);
            RecordOffsets_.push_back(record->Offset);
            DataFileLength_ = record->Offset + record->Length;
            current += sizeof(TChangelogIndexRecord);
        }
        FlushedIndexRecordCount_ = std::ssize(RecordOffsets_);
    }

    return finish();
}

void TFileChangelogIndex::Close()
{
    WaitFor(FlushFuture_)
        .ThrowOnError();

    if (auto handle = std::exchange(Handle_, nullptr)) {
        WaitFor(IOEngine_->Close({.Handle = handle, .Flush = Config_->EnableSync}))
            .ThrowOnError();
    }
}

int TFileChangelogIndex::GetRecordCount() const
{
    return std::ssize(RecordOffsets_);
}

i64 TFileChangelogIndex::GetDataFileLength() const
{
    return DataFileLength_;
}

std::pair<i64, i64> TFileChangelogIndex::GetRecordRange(int index) const
{
    auto recordCount = GetRecordCount();
    auto dataFileLength = GetDataFileLength();
    YT_VERIFY(index >= 0 && index < recordCount);
    YT_VERIFY(dataFileLength >= 0);
    return {
        RecordOffsets_[index],
        index == recordCount - 1 ? dataFileLength : RecordOffsets_[index + 1]
    };
}

std::pair<i64, i64> TFileChangelogIndex::GetRecordsRange(
    int firstRecordIndex,
    int maxRecords,
    i64 maxBytes) const
{
    auto recordCount = GetRecordCount();
    auto dataFileLength = GetDataFileLength();

    YT_VERIFY(firstRecordIndex >= 0 && firstRecordIndex <= recordCount);
    YT_VERIFY(recordCount == 0 || dataFileLength >= 0);

    auto startOffset = firstRecordIndex == recordCount ? dataFileLength : RecordOffsets_[firstRecordIndex];
    i64 endOffset = -1;
    for (int endRecordIndex = firstRecordIndex; endRecordIndex <= recordCount; ++endRecordIndex) {
        endOffset = endRecordIndex == recordCount ? dataFileLength : RecordOffsets_[endRecordIndex];
        auto totalRecords = endRecordIndex - firstRecordIndex;
        auto totalBytes = endOffset - startOffset;
        if (totalRecords >= maxRecords) {
            break;
        }
        if (totalBytes >= maxBytes && totalRecords > 0) {
            break;
        }
    }
    return {startOffset, endOffset};
}

void TFileChangelogIndex::AppendRecord(int index, std::pair<i64, i64> range)
{
    YT_VERIFY(std::ssize(RecordOffsets_) == index);
    YT_VERIFY(DataFileLength_ < 0 || DataFileLength_ == range.first);

    RecordOffsets_.push_back(range.first);
    DataFileLength_ = range.second;
}

void TFileChangelogIndex::SetFlushedDataRecordCount(int count)
{
    YT_VERIFY(count >= FlushedDataRecordCount_);
    YT_VERIFY(count >= FlushedIndexRecordCount_);

    FlushedDataRecordCount_ = count;
}

TFuture<void> TFileChangelogIndex::Flush()
{
    auto recordCount = FlushedDataRecordCount_ - FlushedIndexRecordCount_;
    auto size = sizeof(TChangelogIndexSegmentHeader) + sizeof(TChangelogIndexRecord) * recordCount;
    auto buffer = TSharedMutableRef::Allocate<TFileChangelogIndexTag>(size, /*intializeStorage*/ false);

    auto* current = buffer.Begin();
    auto* header = reinterpret_cast<TChangelogIndexSegmentHeader*>(current);
    header->RecordCount = recordCount;
    header->Padding = 0;
    current += sizeof(TChangelogIndexSegmentHeader);

    for (int index = FlushedIndexRecordCount_; index < std::ssize(RecordOffsets_); ++index) {
        auto* record = reinterpret_cast<TChangelogIndexRecord*>(current);
        auto nextOffset = index == std::ssize(RecordOffsets_) - 1 ? DataFileLength_ : RecordOffsets_[index + 1];
        record->Offset = RecordOffsets_[index];
        record->Length = nextOffset - record->Offset;
        current += sizeof(TChangelogIndexRecord);
    }

    header->Checksum = GetChecksum(buffer.Slice(sizeof(ui64), size));

    YT_LOG_DEBUG("Started flushing changelog file index segment (FirstRecordIndex: %v, RecordCount: %v, IndexFilePosition: %v)",
        FlushedIndexRecordCount_,
        recordCount,
        IndexFilePosition_);

    YT_VERIFY(!Flushing_.exchange(true));

    auto currentPosition = IndexFilePosition_;
    IndexFilePosition_ += size;
    FlushedIndexRecordCount_ += recordCount;

    auto future = IOEngine_->Write({.Handle = Handle_, .Offset = currentPosition, .Buffers = {std::move(buffer)}, .Flush = Config_->EnableSync})
        .Apply(BIND([=, this_ = MakeWeak(this)] {
            YT_VERIFY(Flushing_.exchange(false));
            YT_LOG_DEBUG("Finished flushing changelog file index segment");
        }));

    FlushFuture_ = future;
    return future;
}

bool TFileChangelogIndex::CanFlush() const
{
    return FlushFuture_.IsSet();
}

void TFileChangelogIndex::Clear()
{
    IndexFilePosition_ = 0;
    RecordOffsets_.clear();
    FlushedDataRecordCount_ = 0;
    FlushedIndexRecordCount_ = 0;
    DataFileLength_ = -1;
}

void TFileChangelogIndex::Create()
{
    Clear();

    auto handle = WaitFor(IOEngine_->Open({.Path = FileName_, .Mode = RdWr | CreateAlways}))
        .ValueOrThrow();

    auto buffer = TSharedMutableRef::Allocate<TFileChangelogIndexTag>(sizeof(TChangelogIndexHeader), /*intializeStorage*/ false);

    auto* header = reinterpret_cast<TChangelogIndexHeader*>(buffer.Begin());
    header->Signature = TChangelogIndexHeader::ExpectedSignature;

    WaitFor(IOEngine_->Write({.Handle = handle, .Offset = 0, .Buffers = {std::move(buffer)}}))
        .ThrowOnError();

    Handle_ = std::move(handle);
    IndexFilePosition_ = sizeof(TChangelogIndexHeader);

    YT_LOG_DEBUG("Changelog index file created");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
