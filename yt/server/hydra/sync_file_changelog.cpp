#include "sync_file_changelog.h"
#include "async_file_changelog_index.h"
#include "config.h"
#include "file_helpers.h"
#include "format.h"

#include <yt/ytlib/chunk_client/io_engine.h>
#include <yt/ytlib/hydra/hydra_manager.pb.h>

#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/blob_output.h>
#include <yt/core/misc/checksum.h>
#include <yt/core/misc/fs.h>
#include <yt/core/misc/serialize.h>
#include <yt/core/misc/string.h>

#include <util/system/align.h>

namespace NYT {
namespace NHydra {

using namespace NHydra::NProto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraLogger;
static const auto LockBackoffTime = TDuration::MilliSeconds(100);
static const int MaxLockRetries = 100;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
void ValidateSignature(const T& header)
{
    LOG_FATAL_UNLESS(header.Signature == T::ExpectedSignature,
        "Invalid signature: expected %" PRIx64 ", got %" PRIx64,
        T::ExpectedSignature,
        header.Signature);
}

struct TRecordInfo
{
    TRecordInfo()
        : Id(-1)
        , TotalSize(-1)
    { }

    TRecordInfo(int id, int totalSize)
        : Id(id)
        , TotalSize(totalSize)
    { }

    int Id;
    int TotalSize;

};

//! Tries to read one record from the file.
//! Returns error if failed.
template <class TInput>
TErrorOr<TRecordInfo> TryReadRecord(TInput& input)
{
    // COMPAT(aozeritsky): old format
    int headerSize = sizeof(TChangelogRecordHeader);
    if (input.Avail() < headerSize) {
        return TError("Not enough bytes available in data file to read record header: need %v, got %v",
            sizeof(TChangelogRecordHeader),
            input.Avail());
    }

    int totalSize = 0;
    TChangelogRecordHeader header;

    NFS::ExpectIOErrors([&] () {
        totalSize += ReadPodPadded(input, header);
    });

    if (!input.Success()) {
        return TError("Error reading record header");
    }

    if (header.DataSize <= 0) {
        return TError("Broken record header: data_size < 0");
    }

    struct TSyncChangelogRecordTag { };
    auto data = TSharedMutableRef::Allocate<TSyncChangelogRecordTag>(header.DataSize, false);
    if (input.Avail() < header.DataSize) {
        return TError("Not enough bytes available in data file to read record data: need %v, got %v",
            header.DataSize,
            input.Avail());
    }

    NFS::ExpectIOErrors([&] () {
        totalSize += ReadPadded(input, data);
    });

    if (header.PaddingSize > 0) {
        if (input.Avail() < header.PaddingSize) {
            return TError("Not enough bytes available in data file to read record data: need %v, got %v",
                header.PaddingSize,
                input.Avail());
        }

        NFS::ExpectIOErrors([&] () {
            totalSize += header.PaddingSize;
            input.Skip(header.PaddingSize);
        });
    }

    if (!input.Success()) {
        return TError("Error reading record data");
    }

    auto checksum = GetChecksum(data);
    if (header.Checksum != checksum) {
        return TError("Record data checksum mismatch of record %v (%v != %v)", header.RecordId, header.Checksum, checksum);
    }

    return TRecordInfo(header.RecordId, totalSize);
}

// Computes the length of the maximal valid prefix of index records sequence.
size_t ComputeValidIndexPrefix(
    const std::vector<TChangelogIndexRecord>& index,
    const TChangelogHeader& header,
    TFileWrapper* file)
{
    // Validate index records.
    size_t result = 0;
    for (int i = 0; i < index.size(); ++i) {
        const auto& record = index[i];
        bool correct;
        if (i == 0) {
            correct =
                record.FilePosition == header.HeaderSize &&
                record.RecordId == 0;
        } else {
            const auto& prevRecord = index[i - 1];
            correct =
                record.FilePosition > prevRecord.FilePosition &&
                record.RecordId > prevRecord.RecordId;
        }
        if (!correct) {
            break;
        }
        ++result;
    }

    // Truncate invalid records.
    i64 fileLength = file->GetLength();
    while (result > 0 && index[result - 1].FilePosition > fileLength) {
        --result;
    }

    if (result == 0) {
        return 0;
    }

    // Truncate the last index entry if the corresponding changelog record is corrupt.
    file->Seek(index[result - 1].FilePosition, sSet);
    TCheckedReader<TFileWrapper> changelogReader(*file);
    if (!TryReadRecord(changelogReader).IsOK()) {
        --result;
    }

    return result;
}

} // namespace

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
        , IndexFile_(IOEngine_, fileName + "." + ChangelogIndexExtension, Alignment_, Config_->IndexBlockSize)
        , AppendOutput_(Alignment_, Alignment_)
        , Logger(HydraLogger)
    {
        Logger.AddTag("Path: %v", FileName_);
    }

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

        auto guard = Guard(Lock_);

        Error_.ThrowOnError();
        ValidateNotOpen();

        try {
            // Read and check changelog header.
            TChangelogHeader header;
            std::unique_ptr<TFileWrapper> dataFile;

            NFS::ExpectIOErrors([&] () {
                dataFile.reset(new TFileWrapper(FileName_, RdOnly | Seq | CloseOnExec));

                DataFile_ = IOEngine_->Open(FileName_, RdWr | Seq | CloseOnExec).Get().ValueOrThrow();

                LockDataFile();
                ReadPod(*dataFile, header);
            });

            ValidateSignature(header);

            // Read meta.
            auto serializedMeta = TSharedMutableRef::Allocate(header.MetaSize);

            NFS::ExpectIOErrors([&] () {
                ReadPadded(*dataFile, serializedMeta);
            });

            DeserializeProto(&Meta_, serializedMeta);
            SerializedMeta_ = serializedMeta;

            TruncatedRecordCount_ = header.TruncatedRecordCount == TChangelogHeader::NotTruncatedRecordCount
                ? std::nullopt
                : std::make_optional(header.TruncatedRecordCount);

            ReadIndex(dataFile, header);
            ReadChangelogUntilEnd(dataFile, header);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error opening changelog");
            Error_ = ex;
            throw;
        }

        Open_ = true;

        LOG_DEBUG("Changelog opened (RecordCount: %v, Truncated: %v)",
            RecordCount_,
            TruncatedRecordCount_.operator bool());
    }

    void Close()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        Error_.ThrowOnError();

        if (!Open_) {
            return;
        }

        try {
            NFS::ExpectIOErrors([&] () {
                DataFile_->FlushData();
                DataFile_->Close();

                IndexFile_.Close();
            });
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error closing changelog");
            Error_ = ex;
            throw;
        }

        Open_ = false;

        LOG_DEBUG("Changelog closed");
    }

    void Create(const TChangelogMeta& meta)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        Error_.ThrowOnError();
        ValidateNotOpen();

        try {
            Meta_ = meta;
            SerializedMeta_ = SerializeProtoToRef(Meta_);
            RecordCount_ = 0;

            CreateDataFile();
            IndexFile_.Create();

            CurrentFilePosition_ = DataFile_->GetLength();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error creating changelog");
            Error_ = ex;
            throw;
        }

        Open_ = true;

        LOG_DEBUG("Changelog created");
    }


    const TChangelogMeta& GetMeta() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);
        return Meta_;
    }

    int GetRecordCount() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);
        return RecordCount_;
    }

    i64 GetDataSize() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);
        return CurrentFilePosition_;
    }

    bool IsOpen() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);
        return Open_;
    }


    void Append(
        int firstRecordId,
        const std::vector<TSharedRef>& records)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        Error_.ThrowOnError();
        ValidateOpen();

        YCHECK(!TruncatedRecordCount_);
        YCHECK(firstRecordId == RecordCount_);

        LOG_DEBUG("Started appending to changelog (RecordIds: %v-%v)",
            firstRecordId,
            firstRecordId + records.size() - 1);

        try {
            AppendSizes_.clear();
            AppendSizes_.reserve(records.size());

            AppendOutput_.Clear();

            // Combine records into a single memory blob.
            for (int index = 0; index < records.size(); ++index) {
                const auto& record = records[index];
                YCHECK(!record.Empty());

                int totalSize = 0;
                i64 paddedSize = 0;

                if (index == records.size() - 1) {
                    i64 blockSize = AppendOutput_.Size()
                        + AlignUp(sizeof(TChangelogRecordHeader))
                        + AlignUp(record.Size());

                    paddedSize = ::AlignUp(blockSize, Alignment_) - blockSize;
                }

                YCHECK(paddedSize <= std::numeric_limits<i16>::max());

                TChangelogRecordHeader header(firstRecordId + index, record.Size(), GetChecksum(record), paddedSize);
                totalSize += WritePodPadded(AppendOutput_, header);
                totalSize += WritePadded(AppendOutput_, record);
                totalSize += WriteZeroes(AppendOutput_, paddedSize);

                AppendSizes_.push_back(totalSize);
            }

            YCHECK(::AlignUp(CurrentFilePosition_, Alignment_) == CurrentFilePosition_);
            YCHECK(::AlignUp<i64>(AppendOutput_.Size(), Alignment_) == AppendOutput_.Size());

            TSharedRef data(AppendOutput_.Blob().Begin(), AppendOutput_.Size(), MakeStrong(this));

            // Write blob to file.
            WaitFor(IOEngine_->Pwrite(DataFile_, data, CurrentFilePosition_))
                .ThrowOnError();

            // Process written records (update index etc).
            IndexFile_.Append(firstRecordId, CurrentFilePosition_, AppendSizes_);
            RecordCount_ += records.size();

            for (int index = 0; index < records.size(); ++index) {
                CurrentFilePosition_ += AppendSizes_[index];
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error appending to changelog");
            Error_ = ex;
            throw;
        }

        LOG_DEBUG("Finished appending to changelog");
    }

    void Flush()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        Error_.ThrowOnError();
        ValidateOpen();

        LOG_DEBUG("Started flushing changelog");

        try {
            if (Config_->EnableSync) {
                std::vector<TFuture<void>> futures;
                futures.reserve(2);
                futures.push_back(IndexFile_.FlushData());
                futures.push_back(IOEngine_->FlushData(DataFile_).As<void>());
                WaitFor(Combine(futures)).ThrowOnError();
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error flushing changelog");
            Error_ = ex;
            throw;
        }

        LOG_DEBUG("Finished flushing changelog");
    }

    std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        Error_.ThrowOnError();
        ValidateOpen();

        YCHECK(firstRecordId >= 0);
        YCHECK(maxRecords >= 0);

        LOG_DEBUG("Started reading changelog (FirstRecordId: %v, MaxRecords: %v, MaxBytes: %v)",
            firstRecordId,
            maxRecords,
            maxBytes);

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
                TChangelogRecordHeader header;
                ReadPodPadded(inputStream, header);

                if (header.RecordId != recordId) {
                    THROW_ERROR_EXCEPTION("Record data id mismatch in %v", FileName_)
                        << TErrorAttribute("expected", header.RecordId)
                        << TErrorAttribute("actual", recordId);
                }

                // Save and pad data.
                i64 startOffset = inputStream.Buf() - envelope.Blob.Begin();
                i64 endOffset = startOffset + header.DataSize;

                auto data = envelope.Blob.Slice(startOffset, endOffset);
                inputStream.Skip(AlignUp(header.DataSize));
                inputStream.Skip(header.PaddingSize);

                auto checksum = GetChecksum(data);
                if (header.Checksum != checksum) {
                    THROW_ERROR_EXCEPTION("Record data checksum mismatch in %v", FileName_)
                        << TErrorAttribute("record_id", header.RecordId);
                }

                // Add data to the records.
                if (recordId >= firstRecordId) {
                    records.push_back(data);
                    readBytes += data.Size();
                }
            }
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error reading changelog");
            Error_ = ex;
            throw;
        }

        LOG_DEBUG("Finished reading changelog");
        return records;
    }

    void Truncate(int recordCount)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        Error_.ThrowOnError();
        ValidateOpen();

        YCHECK(recordCount >= 0);
        YCHECK(!TruncatedRecordCount_ || recordCount <= *TruncatedRecordCount_);

        LOG_DEBUG("Started truncating changelog (RecordCount: %v)",
            recordCount);

        try {
            RecordCount_ = recordCount;
            TruncatedRecordCount_ = recordCount;
            UpdateLogHeader();
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error truncating changelog");
            Error_ = ex;
            throw;
        }

        LOG_DEBUG("Finished truncating changelog");
    }

    void Preallocate(size_t size)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(Lock_);

        YCHECK(CurrentFilePosition_ <= size);

        // PB: acually does ftruncate
        DataFile_->Resize(size);
        LOG_DEBUG("Finished preallocating changelog");
    }

private:
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


    //! Checks that the changelog is open. Throws if not.
    void ValidateOpen()
    {
        if (!Open_) {
            THROW_ERROR_EXCEPTION("Changelog is not open");
        }
    }

    //! Checks that the changelog is not open. Throws if it is.
    void ValidateNotOpen()
    {
        if (Open_) {
            THROW_ERROR_EXCEPTION("Changelog is already open");
        }
    }

    //! Flocks the data file, retrying if needed.
    void LockDataFile()
    {
        int index = 0;
        while (true) {
            LOG_DEBUG("Locking data file");
            if (DataFile_->Flock(LOCK_EX | LOCK_NB) == 0) {
                LOG_DEBUG("Data file locked successfullly");
                break;
            } else {
                if (++index >= MaxLockRetries) {
                    THROW_ERROR_EXCEPTION(
                        "Cannot flock %Qv",
                        FileName_) << TError::FromSystem();
                }
                LOG_WARNING("Error locking data file; backing off and retrying");
                WaitFor(TDelayedExecutor::MakeDelayed(LockBackoffTime))
                    .ThrowOnError();
            }
        }
    }

    //! Creates an empty data file.
    void CreateDataFile()
    {
        NFS::ExpectIOErrors([&] () {
            auto tempFileName = FileName_ + NFS::TempFileSuffix;
            TFileWrapper tempFile(tempFileName, WrOnly | CloseOnExec | CreateAlways);

            TChangelogHeader header(
                SerializedMeta_.Size(),
                TChangelogHeader::NotTruncatedRecordCount,
                Alignment_);
            WritePod(tempFile, header);

            Write(tempFile, SerializedMeta_);
            WriteZeroes(tempFile, header.PaddingSize);

            YCHECK(tempFile.GetPosition() == header.HeaderSize);

            tempFile.FlushData();
            tempFile.Close();

            NFS::Replace(tempFileName, FileName_);

            DataFile_ = IOEngine_->Open(FileName_, RdWr | Seq | CloseOnExec).Get().ValueOrThrow();
        });
    }

    //! Rewrites changelog header.
    void UpdateLogHeader()
    {
        NFS::ExpectIOErrors([&] () {
            IOEngine_->FlushData(DataFile_).Get().ValueOrThrow();

            TChangelogHeader header(
                SerializedMeta_.Size(),
                TruncatedRecordCount_ ? *TruncatedRecordCount_ : TChangelogHeader::NotTruncatedRecordCount,
                Alignment_);

            auto data = TAsyncFileChangelogIndex::AllocateAligned<std::nullopt_t>(header.HeaderSize, false, Alignment_);
            ::memcpy(data.Begin(), &header, sizeof(header));
            ::memcpy(data.Begin() + sizeof(header), SerializedMeta_.Begin(), SerializedMeta_.Size());

            IOEngine_->Pwrite(DataFile_, data, 0).Get().ThrowOnError();
            IOEngine_->FlushData(DataFile_).Get().ValueOrThrow();
        });
    }

    //! Reads the maximal valid prefix of index, truncates bad index records.
    void ReadIndex(const std::unique_ptr<TFileWrapper>& dataFile, const TChangelogHeader& header)
    {
        NFS::ExpectIOErrors([&] () {
            auto truncatedRecordCount = header.TruncatedRecordCount == TChangelogHeader::NotTruncatedRecordCount
                ? std::nullopt
                : std::make_optional(header.TruncatedRecordCount);

            IndexFile_.Read(truncatedRecordCount);
            auto correctPrefixSize = ComputeValidIndexPrefix(IndexFile_.Records(), header, &*dataFile);
            IndexFile_.TruncateInvalidRecords(correctPrefixSize);
        });
    }

    //! Reads a piece of changelog containing both #firstRecordId and #lastRecordId.
    TEnvelopeData ReadEnvelope(int firstRecordId, int lastRecordId, i64 maxBytes = -1)
    {
        TEnvelopeData result;

        result.UpperBound = TChangelogIndexRecord(RecordCount_, CurrentFilePosition_);
        IndexFile_.Search(&result.LowerBound, &result.UpperBound, firstRecordId, lastRecordId, maxBytes);

        result.Blob = IOEngine_->Pread(DataFile_, result.GetLength(), result.GetStartPosition()).Get().Value();

        YCHECK(result.Blob.Size() == result.GetLength());

        return result;
    }

    //! Reads changelog starting from the last indexed record until the end of file.
    void ReadChangelogUntilEnd(const std::unique_ptr<TFileWrapper>& dataFile, const TChangelogHeader& header)
    {
        // Extract changelog properties from index.
        i64 fileLength = dataFile->GetLength();
        if (IndexFile_.IsEmpty()) {
            RecordCount_ = 0;
            CurrentFilePosition_ = header.HeaderSize;
        } else {
            // Record count would be set below.
            CurrentFilePosition_ = IndexFile_.LastRecord().FilePosition;
        }

        // Seek to proper position in file, initialize checkable reader.
        NFS::ExpectIOErrors([&] () {
            dataFile->Seek(CurrentFilePosition_, sSet);
        });

        TCheckedReader<TFileWrapper> dataReader(*dataFile);
        std::optional<TRecordInfo> lastCorrectRecordInfo;

        if (!IndexFile_.IsEmpty()) {
            // Skip the first index record.
            // It must be correct since we have already checked the index.
            auto recordInfoOrError = TryReadRecord(dataReader);
            YCHECK(recordInfoOrError.IsOK());
            const auto& recordInfo = recordInfoOrError.Value();
            RecordCount_ = IndexFile_.LastRecord().RecordId + 1;
            CurrentFilePosition_ += recordInfo.TotalSize;

            lastCorrectRecordInfo = recordInfoOrError.Value();
        }

        while (CurrentFilePosition_ < fileLength) {
            auto recordInfoOrError = TryReadRecord(dataReader);
            if (!recordInfoOrError.IsOK()) {
                if (TruncatedRecordCount_ && RecordCount_ < *TruncatedRecordCount_) {
                    THROW_ERROR_EXCEPTION("Broken record found in truncated changelog %v",
                        FileName_)
                        << TErrorAttribute("record_id", RecordCount_)
                        << TErrorAttribute("offset", CurrentFilePosition_)
                        << recordInfoOrError;
                }

                LOG_WARNING(recordInfoOrError, "Broken record found in changelog, trimmed (RecordId: %v, Offset: %v)",
                    RecordCount_,
                    CurrentFilePosition_);
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

            lastCorrectRecordInfo = recordInfoOrError.Value();

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

        auto correctSize = ::AlignUp<i64>(CurrentFilePosition_, Alignment_);
        // rewrite the last 4K-block in case of incorrect size?
        if (correctSize > CurrentFilePosition_) {
            YCHECK(lastCorrectRecordInfo);

            auto totalRecordSize =  lastCorrectRecordInfo->TotalSize;
            auto offset = CurrentFilePosition_ - totalRecordSize;
            TFileWrapper file(FileName_, RdWr);
            TChangelogRecordHeader header;

            file.Seek(offset, sSet);
            ReadPod(file, header);
            header.PaddingSize = correctSize - CurrentFilePosition_;

            file.Seek(offset, sSet);
            WritePod(file, header);
            file.Resize(correctSize);
            file.FlushData();
            file.Close();
            CurrentFilePosition_ = correctSize;
        }

        YCHECK(correctSize == CurrentFilePosition_);
    }

    template <class TOutput>
    int WriteZeroes(TOutput& output, int count)
    {
        int written = 0;

        while (written < count) {
            int toWrite = Min<int>(ZeroBuffer_.Size(), count - written);
            output.Write(ZeroBuffer_.Begin(), toWrite);
            written += toWrite;
        }

        YCHECK(written == count);

        return written;
    }

    const NChunkClient::IIOEnginePtr IOEngine_;

    const TString FileName_;
    const TFileChangelogConfigPtr Config_;
    const i64 Alignment_ = 4096;

    TError Error_;
    bool Open_ = false;
    int RecordCount_ = -1;
    std::optional<int> TruncatedRecordCount_;
    i64 CurrentFilePosition_ = -1;

    TChangelogMeta Meta_;
    TSharedRef SerializedMeta_;

    std::shared_ptr<TFileHandle> DataFile_;
    TAsyncFileChangelogIndex IndexFile_;

    // Reused by Append.
    std::vector<int> AppendSizes_;
    TBlobOutput AppendOutput_;
    const TBlob ZeroBuffer_ = TBlob(TDefaultBlobTag(), 1<<16, true);

    //! Auxiliary data.
    //! Protects file resources.
    mutable TSpinLock Lock_;
    NLogging::TLogger Logger;

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

void TSyncFileChangelog::Create(const TChangelogMeta& meta)
{
    Impl_->Create(meta);
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

const TChangelogMeta& TSyncFileChangelog::GetMeta() const
{
    return Impl_->GetMeta();
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

} // namespace NHydra
} // namespace NYT
