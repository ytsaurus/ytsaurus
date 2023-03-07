#include "sync_file_changelog.h"
#include "async_file_changelog_index.h"
#include "config.h"
#include "file_helpers.h"
#include "format.h"

#include <yt/ytlib/chunk_client/io_engine.h>

#include <yt/ytlib/hydra/proto/hydra_manager.pb.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/blob_output.h>
#include <yt/core/misc/checksum.h>
#include <yt/core/misc/fs.h>
#include <yt/core/misc/serialize.h>
#include <yt/core/misc/string.h>

#include <yt/core/tracing/trace_context.h>

#include <util/system/align.h>
#include <util/system/mutex.h>
#include <util/system/flock.h>
#include <util/system/align.h>

namespace NYT::NHydra {

using namespace NHydra::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto LockBackoffTime = TDuration::MilliSeconds(100);
static constexpr int MaxLockRetries = 100;
static constexpr i64 Alignment = 4_KB;

////////////////////////////////////////////////////////////////////////////////

class TSyncFileChangelog::TImpl
    : public TIntrinsicRefCounted
{
public:
    TImpl(
        const NChunkClient::IIOEnginePtr& ioEngine,
        const TString& fileName,
        TFileChangelogConfigPtr config)
        : IOEngine_(ioEngine)
        , FileName_(fileName)
        , Config_(config)
        , Logger(NLogging::TLogger(HydraLogger)
            .AddTag("Path: %v", FileName_))
        , IndexFile_(IOEngine_, fileName + "." + ChangelogIndexExtension, Alignment, Config_->IndexBlockSize)
        , AppendOutput_(Alignment, Alignment)
    { }

    const TFileChangelogConfigPtr& GetConfig() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Config_;
    }

    const TString& GetFileName() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return FileName_;
    }


    void Open()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Mutex_);

        Error_.ThrowOnError();
        ValidateNotOpen();

        try {
            std::unique_ptr<TFileWrapper> dataFile;
            NFS::ExpectIOErrors([&] {
                dataFile.reset(new TFileWrapper(FileName_, RdOnly | Seq | CloseOnExec));
                DataFile_ = IOEngine_->Open(FileName_, RdWr | Seq | CloseOnExec).Get().ValueOrThrow();
                LockDataFile();
            });

            // Read and check changelog header.
            ui64 signature;
            NFS::ExpectIOErrors([&] {
                ReadPod(*dataFile, signature);
                dataFile->Seek(0, sSet);
            });

            switch (signature) {
                case TChangelogHeader_4::ExpectedSignature:
                    Format_ = EFileChangelogFormat::V4;
                    FileHeaderSize_ = sizeof(TChangelogHeader_4);
                    RecordHeaderSize_ = sizeof(TChangelogRecordHeader_4);
                    break;
                case TChangelogHeader_5::ExpectedSignature:
                    Format_ = EFileChangelogFormat::V5;
                    FileHeaderSize_ = sizeof(TChangelogHeader_5);
                    RecordHeaderSize_ = sizeof(TChangelogRecordHeader_5);
                    break;
                default:
                    YT_LOG_FATAL_UNLESS("Invalid changelog signature %" PRIx64,
                        signature);
            }

            TChangelogHeader header;
            Zero(header);
            NFS::ExpectIOErrors([&] {
                dataFile->Seek(0, sSet);
                if (dataFile->Load(&header, FileHeaderSize_) != FileHeaderSize_) {
                    THROW_ERROR_EXCEPTION(
                        NHydra::EErrorCode::ChangelogIOError,
                        "Changelog header cannot be read");
                }
            });

            // Parse Uuid_.
            switch (Format_) {
                case EFileChangelogFormat::V4:
                    break;
                case EFileChangelogFormat::V5:
                    Uuid_ = header.Uuid;
                    break;
                default:
                    YT_ABORT();
            }

            // Parse TruncatedRecordCount_.
            TruncatedRecordCount_ = header.TruncatedRecordCount == TChangelogHeader::NotTruncatedRecordCount
                ? std::nullopt
                : std::make_optional(header.TruncatedRecordCount);

            // Parse meta.
            struct TMetaTag { };
            auto serializedMeta = TSharedMutableRef::Allocate<TMetaTag>(header.MetaSize);
            NFS::ExpectIOErrors([&] {
                ReadRefPadded(*dataFile, serializedMeta);
            });

            ReadIndex(dataFile.get(), header.FirstRecordOffset);
            ReadChangelogUntilEnd(dataFile.get(), header.FirstRecordOffset);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error opening changelog");
            Error_ = ex;
            Cleanup();
            throw;
        }

        Open_ = true;

        YT_LOG_DEBUG("Changelog opened (RecordCount: %v, TruncatedRecordCount: %v, Format: %v)",
            RecordCount_.load(),
            TruncatedRecordCount_,
            Format_);
    }

    void Close()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Mutex_);

        Error_.ThrowOnError();

        if (!Open_) {
            return;
        }

        Cleanup();

        try {
            NFS::ExpectIOErrors([&] () {
                {
                    NTracing::TNullTraceContextGuard nullTraceContextGuard;
                    DataFile_->FlushData();
                    DataFile_->Close();
                }
                IndexFile_.Close();
            });
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error closing changelog");
            Error_ = ex;
            throw;
        }

        YT_LOG_DEBUG("Changelog closed");
    }

    void Create(EFileChangelogFormat format)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Mutex_);

        Error_.ThrowOnError();
        ValidateNotOpen();

        try {
            Format_ = format;
            Uuid_ = TGuid::Create();
            RecordCount_ = 0;
            TruncatedRecordCount_.reset();

            CreateDataFile();
            IndexFile_.Create();

            CurrentFilePosition_ = DataFile_->GetLength();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error creating changelog");
            Error_ = ex;
            throw;
        }

        Open_ = true;

        YT_LOG_DEBUG("Changelog created");
    }


    int GetRecordCount() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return RecordCount_;
    }

    i64 GetDataSize() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return CurrentFilePosition_;
    }

    bool IsOpen() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Open_;
    }


    void Append(
        int firstRecordId,
        const std::vector<TSharedRef>& records)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Mutex_);

        Error_.ThrowOnError();
        ValidateOpen();

        YT_VERIFY(!TruncatedRecordCount_);
        YT_VERIFY(firstRecordId == RecordCount_);

        YT_LOG_DEBUG("Started appending to changelog (RecordIds: %v-%v)",
            firstRecordId,
            firstRecordId + records.size() - 1);

        switch (Format_) {
            case EFileChangelogFormat::V4:
                DoAppend<TChangelogRecordHeader_4>(firstRecordId, records);
                break;
            case EFileChangelogFormat::V5:
                DoAppend<TChangelogRecordHeader_5>(firstRecordId, records);
                break;
            default:
                YT_ABORT();
        }
    }

    void Flush()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Mutex_);

        Error_.ThrowOnError();
        ValidateOpen();

        YT_LOG_DEBUG("Started flushing changelog");

        try {
            if (Config_->EnableSync) {
                std::vector<TFuture<void>> futures;
                futures.reserve(2);
                futures.push_back(IndexFile_.FlushData());
                futures.push_back(IOEngine_->FlushData(DataFile_).As<void>());
                WaitFor(Combine(futures)).ThrowOnError();
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error flushing changelog");
            Error_ = ex;
            throw;
        }

        YT_LOG_DEBUG("Finished flushing changelog");
    }

    std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Mutex_);

        Error_.ThrowOnError();
        ValidateOpen();

        YT_VERIFY(firstRecordId >= 0);
        YT_VERIFY(maxRecords >= 0);

        YT_LOG_DEBUG("Started reading changelog (FirstRecordId: %v, MaxRecords: %v, MaxBytes: %v)",
            firstRecordId,
            maxRecords,
            maxBytes);

        switch (Format_) {
            case EFileChangelogFormat::V4:
                return DoRead<TChangelogRecordHeader_4>(firstRecordId, maxRecords, maxBytes);
            case EFileChangelogFormat::V5:
                return DoRead<TChangelogRecordHeader_5>(firstRecordId, maxRecords, maxBytes);
            default:
                YT_ABORT();
        }
    }

    void Truncate(int recordCount)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Mutex_);

        Error_.ThrowOnError();
        ValidateOpen();

        YT_VERIFY(recordCount >= 0);
        YT_VERIFY(!TruncatedRecordCount_ || recordCount <= *TruncatedRecordCount_);

        YT_LOG_DEBUG("Started truncating changelog (RecordCount: %v)",
            recordCount);

        try {
            RecordCount_ = recordCount;
            TruncatedRecordCount_ = recordCount;
            UpdateLogHeader();
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error truncating changelog");
            Error_ = ex;
            throw;
        }

        YT_LOG_DEBUG("Finished truncating changelog");
    }

    void Preallocate(size_t size)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Mutex_);

        YT_VERIFY(CurrentFilePosition_ <= size);

        WaitFor(IOEngine_->Fallocate(DataFile_, size))
            .ThrowOnError();

        YT_LOG_DEBUG("Finished preallocating changelog");
    }

private:
    const NChunkClient::IIOEnginePtr IOEngine_;
    const TString FileName_;
    const TFileChangelogConfigPtr Config_;
    const NLogging::TLogger Logger;

    TSysMutex Mutex_;
    TError Error_;
    std::atomic<bool> Open_ = false;
    EFileChangelogFormat Format_ = EFileChangelogFormat::V5;
    int FileHeaderSize_ = -1;
    int RecordHeaderSize_ = -1;
    std::optional<TGuid> Uuid_;
    std::atomic<int> RecordCount_ = -1;
    std::optional<int> TruncatedRecordCount_;
    std::atomic<i64> CurrentFilePosition_ = -1;

    std::shared_ptr<TFileHandle> DataFile_;
    TAsyncFileChangelogIndex IndexFile_;

    // Reused by Append.
    std::vector<int> AppendSizes_;
    TBlobOutput AppendOutput_;


    struct TEnvelopeData
    {
        i64 GetLength() const
        {
            return UpperBound.FilePosition - LowerBound.FilePosition;
        }

        i64 GetStartPosition() const
        {
            return LowerBound.FilePosition;
        }

        i64 GetStartRecordId() const
        {
            return LowerBound.RecordId;
        }

        i64 GetEndRecordId() const
        {
            return UpperBound.RecordId;
        }

        TChangelogIndexRecord LowerBound;
        TChangelogIndexRecord UpperBound;
        TSharedMutableRef Blob;
    };

    //! Resets mutable state to default values.
    void Cleanup()
    {
        Open_ = false;
        Format_ = EFileChangelogFormat::V5;
        FileHeaderSize_ = -1;
        RecordHeaderSize_ = -1;
        Uuid_.reset();
        RecordCount_ = -1;
        TruncatedRecordCount_.reset();
        CurrentFilePosition_ = -1;
    }

    //! Checks that the changelog is open. Throws if not.
    void ValidateOpen()
    {
        if (!Open_) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidChangelogState,
                "Changelog is not open");
        }
    }

    //! Checks that the changelog is not open. Throws if it is.
    void ValidateNotOpen()
    {
        if (Open_) {
            THROW_ERROR_EXCEPTION(
                NHydra::EErrorCode::InvalidChangelogState,
                "Changelog is already open");
        }
    }

    //! Flocks the data file, retrying if needed.
    void LockDataFile()
    {
        int index = 0;
        while (true) {
            YT_LOG_DEBUG("Locking data file");
            if (DataFile_->Flock(LOCK_EX | LOCK_NB) == 0) {
                YT_LOG_DEBUG("Data file locked successfullly");
                break;
            }

            auto error = TError::FromSystem();

            if (++index >= MaxLockRetries) {
                THROW_ERROR_EXCEPTION(
                    NHydra::EErrorCode::ChangelogIOError,
                    "Cannot flock %Qv",
                    FileName_)
                    << error;
            }

            YT_LOG_WARNING(error, "Error locking data file; backing off and retrying");
            TDelayedExecutor::WaitForDuration(LockBackoffTime);
        }
    }

    //! Builds the changelog header representing its current state.
    template <class TFileHeader>
    TFileHeader MakeChangelogHeader()
    {
        TFileHeader header;
        Zero(header);
        header.Signature = TFileHeader::ExpectedSignature;
        header.MetaSize = 0;
        header.FirstRecordOffset = ::AlignUp<size_t>(sizeof(TFileHeader) + header.MetaSize, Alignment);
        header.TruncatedRecordCount = TruncatedRecordCount_.value_or(TChangelogHeader::NotTruncatedRecordCount);
        header.PaddingSize = header.FirstRecordOffset - sizeof(TFileHeader) - header.MetaSize;
        if constexpr(std::is_base_of_v<TChangelogHeader_5, TFileHeader>) {
            header.Uuid = *Uuid_;
        }
        return header;
    }

    //! Creates an empty data file.
    template <class TFileHeader, class TRecordHeader>
    void DoCreateDataFile()
    {
        FileHeaderSize_ = sizeof(TFileHeader);
        RecordHeaderSize_ = sizeof(TRecordHeader);

        NFS::ExpectIOErrors([&] {
            {
                NTracing::TNullTraceContextGuard nullTraceContextGuard;

                auto tempFileName = FileName_ + NFS::TempFileSuffix;
                TFileWrapper tempFile(tempFileName, WrOnly | CloseOnExec | CreateAlways);

                auto header = MakeChangelogHeader<TFileHeader>();
                WritePod(tempFile, header);

                // COMPAT(aleksandra-zh)
                WriteRef(tempFile, TRef());
                WriteZeroes(tempFile, header.PaddingSize);

                YT_VERIFY(tempFile.GetPosition() == header.FirstRecordOffset);

                tempFile.FlushData();
                tempFile.Close();

                NFS::Replace(tempFileName, FileName_);
            }

            DataFile_ = IOEngine_->Open(FileName_, RdWr | Seq | CloseOnExec).Get().ValueOrThrow();
        });
    }

    //! Creates an empty data file.
    void CreateDataFile()
    {
        switch (Format_) {
            case EFileChangelogFormat::V4:
                DoCreateDataFile<TChangelogHeader_4, TChangelogRecordHeader_4>();
                break;
            case EFileChangelogFormat::V5:
                DoCreateDataFile<TChangelogHeader_5, TChangelogRecordHeader_5>();
                break;
            default:
                YT_ABORT();
        }
    }

    //! Rewrites changelog header of a given type.
    template <class T>
    void DoUpdateLogHeader()
    {
        NFS::ExpectIOErrors([&] {
            IOEngine_->FlushData(DataFile_).Get().ValueOrThrow();

            auto header = MakeChangelogHeader<T>();
            auto data = TAsyncFileChangelogIndex::AllocateAligned(header.FirstRecordOffset, false, Alignment);
            ::memcpy(data.Begin(), &header, sizeof(header));

            IOEngine_->Pwrite(DataFile_, data, 0).Get().ThrowOnError();
            IOEngine_->FlushData(DataFile_).Get().ValueOrThrow();
        });
    }

    //! Rewrites changelog header choosing the appropriate type.
    void UpdateLogHeader()
    {
        switch (Format_) {
            case EFileChangelogFormat::V4:
                DoUpdateLogHeader<TChangelogHeader_4>();
                break;
            case EFileChangelogFormat::V5:
                DoUpdateLogHeader<TChangelogHeader_5>();
                break;
            default:
                YT_ABORT();
        }
    }

    //! Reads the maximal valid prefix of index, truncates bad index records.
    void ReadIndex(TFileWrapper* dataFile, i64 firstRecordOffset)
    {
        NFS::ExpectIOErrors([&] {
            IndexFile_.Read(TruncatedRecordCount_);
            auto validPrefixSize = ComputeValidIndexPrefix(dataFile, firstRecordOffset);
            IndexFile_.TruncateInvalidRecords(validPrefixSize);
        });
    }

    //! Reads a piece of changelog containing both #firstRecordId and #lastRecordId.
    TEnvelopeData ReadEnvelope(int firstRecordId, int lastRecordId, i64 maxBytes = -1)
    {
        TEnvelopeData result;

        Zero(result.UpperBound);
        result.UpperBound.RecordId = RecordCount_;
        result.UpperBound.FilePosition = CurrentFilePosition_;
        IndexFile_.Search(&result.LowerBound, &result.UpperBound, firstRecordId, lastRecordId, maxBytes);

        result.Blob = IOEngine_->Pread(DataFile_, result.GetLength(), result.GetStartPosition()).Get().Value();

        YT_VERIFY(result.Blob.Size() == result.GetLength());

        return result;
    }

    //! Reads changelog starting from the last indexed record until the end of file.
    void ReadChangelogUntilEnd(TFileWrapper* dataFile, i64 firstRecordOffset)
    {
        // Extract changelog properties from index.
        i64 fileLength = dataFile->GetLength();
        if (IndexFile_.IsEmpty()) {
            RecordCount_ = 0;
            CurrentFilePosition_ = firstRecordOffset;
        } else {
            // Record count would be set below.
            CurrentFilePosition_ = IndexFile_.LastRecord().FilePosition;
        }

        // Seek to proper position in file, initialize checkable reader.
        NFS::ExpectIOErrors([&] {
            dataFile->Seek(CurrentFilePosition_, sSet);
        });

        TCheckedReader<TFileWrapper> dataReader(*dataFile);
        std::optional<TRecordInfo> lastValidRecordInfo;

        if (!IndexFile_.IsEmpty()) {
            // Skip the first index record.
            // It must be valid since we have already checked the index.
            auto recordInfoOrError = TryReadRecord(dataReader);
            YT_VERIFY(recordInfoOrError.IsOK());
            const auto& recordInfo = recordInfoOrError.Value();
            RecordCount_ = IndexFile_.LastRecord().RecordId + 1;
            CurrentFilePosition_ += recordInfo.TotalSize;

            lastValidRecordInfo = recordInfoOrError.Value();
        }

        while (CurrentFilePosition_ < fileLength) {
            auto recordInfoOrError = TryReadRecord(dataReader);
            if (!recordInfoOrError.IsOK()) {
                if (TruncatedRecordCount_ && RecordCount_ < *TruncatedRecordCount_) {
                    THROW_ERROR_EXCEPTION(
                        NHydra::EErrorCode::BrokenChangelog,
                        "Broken record found in truncated changelog %v",
                        FileName_)
                        << TErrorAttribute("record_id", RecordCount_)
                        << TErrorAttribute("offset", CurrentFilePosition_)
                        << recordInfoOrError;
                }

                YT_LOG_WARNING(recordInfoOrError, "Broken record found in changelog, trimmed (RecordId: %v, Offset: %v)",
                    RecordCount_.load(),
                    CurrentFilePosition_.load());
                break;
            }

            const auto& recordInfo = recordInfoOrError.Value();
            if (recordInfo.Id != RecordCount_) {
                THROW_ERROR_EXCEPTION("Mismatched record id found in changelog %v",
                    FileName_)
                    << TErrorAttribute("expected_record_id", RecordCount_)
                    << TErrorAttribute("actual_record_id", recordInfoOrError.Value().Id)
                    << TErrorAttribute("offset", CurrentFilePosition_);
            }

            lastValidRecordInfo = recordInfoOrError.Value();

            if (TruncatedRecordCount_ && RecordCount_ == *TruncatedRecordCount_) {
                break;
            }

            auto recordId = recordInfoOrError.Value().Id;
            auto recordSize = recordInfoOrError.Value().TotalSize;
            IndexFile_.Append(recordId, CurrentFilePosition_, recordSize);
            RecordCount_ += 1;
            CurrentFilePosition_ += recordSize;
        }

        if (TruncatedRecordCount_) {
            return;
        }

        IndexFile_.FlushData().Get().ThrowOnError();

        auto validSize = ::AlignUp<i64>(CurrentFilePosition_.load(), Alignment);
        // Rewrite the last 4K-block in case of incorrect size?
        if (validSize > CurrentFilePosition_) {
            YT_VERIFY(lastValidRecordInfo);

            auto totalRecordSize =  lastValidRecordInfo->TotalSize;
            auto offset = CurrentFilePosition_ - totalRecordSize;

            // NB: Only overwrite the basic (v4) part of the header.
            TChangelogRecordHeader_4 header;
            Zero(header);

            TFileWrapper file(FileName_, RdWr);
            file.Seek(offset, sSet);
            if (file.Load(&header, sizeof(header)) != sizeof(header)) {
                THROW_ERROR_EXCEPTION(
                    NHydra::EErrorCode::ChangelogIOError,
                    "Record header cannot be read");
            }

            header.PaddingSize = validSize - CurrentFilePosition_;

            file.Seek(offset, sSet);
            WritePod(file, header);
            file.Resize(validSize);
            file.FlushData();
            file.Close();

            CurrentFilePosition_ = validSize;
        }

        YT_VERIFY(validSize == CurrentFilePosition_);
    }

    struct TRecordInfo
    {
        int Id;
        int TotalSize;
    };

    //! Tries to read one record from the file.
    //! Returns error if failed.
    template <class TRecordHeader, class TInput>
    TErrorOr<TRecordInfo> DoTryReadRecord(TInput& input)
    {
        int totalSize = 0;
        TRecordHeader header;

        if (input.Avail() < sizeof(header)) {
            return TError("Not enough bytes available in data file to read record header: expected %v, got %v",
                sizeof(header),
                input.Avail());
        }

        NFS::ExpectIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            totalSize += ReadPodPadded(input, header);
        });

        if (!input.Success()) {
            return TError("Error reading record header");
        }

        if (header.DataSize <= 0) {
            return TError("Broken record header: DataSize <= 0");
        }

        struct TSyncChangelogRecordTag { };
        auto data = TSharedMutableRef::Allocate<TSyncChangelogRecordTag>(header.DataSize, false);
        if (input.Avail() < header.DataSize) {
            return TError("Not enough bytes available in data file to read record data: expected %v, got %v",
                header.DataSize,
                input.Avail());
        }

        NFS::ExpectIOErrors([&] {
            NTracing::TNullTraceContextGuard nullTraceContextGuard;
            totalSize += ReadRefPadded(input, data);
        });

        if (header.PaddingSize > 0) {
            if (input.Avail() < header.PaddingSize) {
                return TError("Not enough bytes available in data file to read record data: expected %v, got %v",
                    header.PaddingSize,
                    input.Avail());
            }

            NFS::ExpectIOErrors([&] {
                NTracing::TNullTraceContextGuard nullTraceContextGuard;
                totalSize += header.PaddingSize;
                input.Skip(header.PaddingSize);
            });
        }

        if (!input.Success()) {
            return TError("Error reading record data");
        }

        if constexpr(std::is_base_of_v<TChangelogRecordHeader_5, TRecordHeader>) {
            if (Uuid_ && header.ChangelogUuid != *Uuid_) {
                return TError("Changelog UUID mismatch in record %v: %v != %v",
                    header.RecordId,
                    header.ChangelogUuid,
                    Uuid_);
            }
        }

        auto checksum = GetChecksum(data);
        if (header.Checksum != checksum) {
            return TError("Data checksum mismatch in record %v: %" PRIx64 "!= %" PRIx64,
                header.RecordId,
                header.Checksum,
                checksum);
        }

        return TRecordInfo{header.RecordId, totalSize};
    }

    //! Tries to read one record from the file.
    //! Returns error if failed.
    template <class TInput>
    TErrorOr<TRecordInfo> TryReadRecord(TInput& input)
    {
        switch (Format_) {
            case EFileChangelogFormat::V4:
                return DoTryReadRecord<TChangelogRecordHeader_4>(input);
            case EFileChangelogFormat::V5:
                return DoTryReadRecord<TChangelogRecordHeader_5>(input);
            default:
                YT_ABORT();
        }
    }

    // Computes the length of the maximal valid prefix of index records sequence.
    int ComputeValidIndexPrefix(TFileWrapper* file, i64 firstRecordOffset)
    {
        // Validate index records.
        int result = 0;
        const auto& records = IndexFile_.Records();
        for (int i = 0; i < records.size(); ++i) {
            const auto& record = records[i];
            bool valid;
            if (i == 0) {
                valid = record.FilePosition == firstRecordOffset && record.RecordId == 0;
            } else {
                const auto& prevRecord = records[i - 1];
                valid =
                    record.FilePosition > prevRecord.FilePosition &&
                    record.RecordId > prevRecord.RecordId;
            }
            if (!valid) {
                break;
            }
            ++result;
        }

        // Truncate invalid records.
        i64 fileLength = file->GetLength();
        while (result > 0 && records[result - 1].FilePosition > fileLength) {
            --result;
        }

        if (result == 0) {
            return 0;
        }

        // Truncate the last index entry if the corresponding changelog record is corrupt.
        file->Seek(records[result - 1].FilePosition, sSet);
        TCheckedReader<TFileWrapper> changelogReader(*file);
        if (!TryReadRecord(changelogReader).IsOK()) {
            --result;
        }

        return result;
    }

    template <class TRecordHeader>
    void DoAppend(
        int firstRecordId,
        const std::vector<TSharedRef>& records)
    {
        i64 bytesWritten = 0;

        try {
            AppendSizes_.clear();
            AppendSizes_.reserve(records.size());

            AppendOutput_.Clear();

            // Combine records into a single memory blob.
            for (int index = 0; index < records.size(); ++index) {
                const auto& record = records[index];
                YT_VERIFY(!record.Empty());

                int totalSize = 0;
                i64 paddingSize = 0;

                if (index == records.size() - 1) {
                    i64 blockSize =
                        AppendOutput_.Size() +
                        AlignUp(sizeof(TRecordHeader)) +
                        AlignUp(record.Size());
                    paddingSize = ::AlignUp(blockSize, Alignment) - blockSize;
                }

                YT_VERIFY(paddingSize <= std::numeric_limits<i16>::max());

                TRecordHeader header;
                Zero(header);
                header.RecordId = firstRecordId + index;
                header.DataSize = record.Size();
                header.Checksum = GetChecksum(record);
                header.PaddingSize = paddingSize;
                if constexpr(std::is_base_of_v<TChangelogRecordHeader_5, TRecordHeader>) {
                    header.ChangelogUuid = *Uuid_;
                }

                totalSize += WritePodPadded(AppendOutput_, header);
                totalSize += WriteRefPadded(AppendOutput_, record);
                totalSize += WriteZeroes(AppendOutput_, paddingSize);

                AppendSizes_.push_back(totalSize);
            }

            YT_VERIFY(::AlignUp(CurrentFilePosition_.load(), Alignment) == CurrentFilePosition_);
            YT_VERIFY(::AlignUp<i64>(AppendOutput_.Size(), Alignment) == AppendOutput_.Size());

            TSharedRef data(AppendOutput_.Blob().Begin(), AppendOutput_.Size(), MakeStrong(this));

            // Write blob to file.
            WaitFor(IOEngine_->Pwrite(DataFile_, data, CurrentFilePosition_))
                .ThrowOnError();

            // Process written records (update index etc).
            IndexFile_.Append(firstRecordId, CurrentFilePosition_, AppendSizes_);
            RecordCount_ += records.size();
            bytesWritten = data.Size();

            for (int index = 0; index < records.size(); ++index) {
                CurrentFilePosition_ += AppendSizes_[index];
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error appending to changelog");
            Error_ = ex;
            throw;
        }

        YT_LOG_DEBUG("Finished appending to changelog (BytesWritten: %v)", bytesWritten);
    }

    template <class TRecordHeader>
    std::vector<TSharedRef> DoRead(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes)
    {
        std::vector<TSharedRef> records;

        try {
            // Prevent search in empty index.
            if (IndexFile_.IsEmpty()) {
                return records;
            }

            maxRecords = std::min(maxRecords, RecordCount_ - firstRecordId);
            int lastRecordId = firstRecordId + maxRecords; // non-inclusive

            // Read envelope piece of changelog.
            auto envelope = ReadEnvelope(firstRecordId, lastRecordId, std::min(IndexFile_.LastRecord().FilePosition, maxBytes));

            // Read records from envelope data and save them to the records.
            i64 readBytes = 0;
            TMemoryInput inputStream(envelope.Blob.Begin(), envelope.GetLength());
            for (i64 recordId = envelope.GetStartRecordId();
                recordId < envelope.GetEndRecordId() && recordId < lastRecordId && readBytes < maxBytes;
                ++recordId)
            {
                // Read and check header.
                TRecordHeader header;
                ReadPodPadded(inputStream, header);

                if (header.RecordId != recordId) {
                    THROW_ERROR_EXCEPTION(
                        NHydra::EErrorCode::BrokenChangelog,
                        "Record data id mismatch in %v", FileName_)
                        << TErrorAttribute("expected", header.RecordId)
                        << TErrorAttribute("actual", recordId);
                }

                // Save and pad data.
                i64 startOffset = inputStream.Buf() - envelope.Blob.Begin();
                i64 endOffset = startOffset + header.DataSize;

                auto data = envelope.Blob.Slice(startOffset, endOffset);
                inputStream.Skip(AlignUp<size_t>(header.DataSize, SerializationAlignment));
                inputStream.Skip(header.PaddingSize);

                auto checksum = GetChecksum(data);
                if (header.Checksum != checksum) {
                    THROW_ERROR_EXCEPTION(
                        NHydra::EErrorCode::BrokenChangelog,
                        "Record data checksum mismatch in %v", FileName_)
                        << TErrorAttribute("record_id", header.RecordId);
                }

                // Add data to the records.
                if (recordId >= firstRecordId) {
                    records.push_back(data);
                    readBytes += data.Size();
                }
            }
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error reading changelog");
            Error_ = ex;
            throw;
        }

        YT_LOG_DEBUG("Finished reading changelog");
        return records;
    }
};

////////////////////////////////////////////////////////////////////////////////

TSyncFileChangelog::TSyncFileChangelog(
    const NChunkClient::IIOEnginePtr& ioEngine,
    const TString& fileName,
    TFileChangelogConfigPtr config)
    : Impl_(New<TImpl>(
        ioEngine,
        fileName,
        config))
{ }

TSyncFileChangelog::~TSyncFileChangelog() = default;

const TFileChangelogConfigPtr& TSyncFileChangelog::GetConfig()
{
    return Impl_->GetConfig();
}

const TString& TSyncFileChangelog::GetFileName() const
{
    return Impl_->GetFileName();
}

void TSyncFileChangelog::Open()
{
    Impl_->Open();
}

void TSyncFileChangelog::Close()
{
    Impl_->Close();
}

void TSyncFileChangelog::Create(EFileChangelogFormat format)
{
    Impl_->Create(format);
}

int TSyncFileChangelog::GetRecordCount() const
{
    return Impl_->GetRecordCount();
}

i64 TSyncFileChangelog::GetDataSize() const
{
    return Impl_->GetDataSize();
}

bool TSyncFileChangelog::IsOpen() const
{
    return Impl_->IsOpen();
}

void TSyncFileChangelog::Append(
    int firstRecordId,
    const std::vector<TSharedRef>& records)
{
    Impl_->Append(firstRecordId, records);
}

void TSyncFileChangelog::Flush()
{
    Impl_->Flush();
}

std::vector<TSharedRef> TSyncFileChangelog::Read(
    int firstRecordId,
    int maxRecords,
    i64 maxBytes)
{
    return Impl_->Read(firstRecordId, maxRecords, maxBytes);
}

void TSyncFileChangelog::Truncate(int recordCount)
{
    Impl_->Truncate(recordCount);
}

void TSyncFileChangelog::Preallocate(size_t size)
{
    return Impl_->Preallocate(size);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
