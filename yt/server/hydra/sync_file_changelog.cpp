#include "stdafx.h"
#include "sync_file_changelog.h"
#include "config.h"
#include "format.h"
#include "file_helpers.h"

#include <core/misc/fs.h>
#include <core/misc/string.h>
#include <core/misc/serialize.h>
#include <core/misc/blob_output.h>
#include <core/misc/checksum.h>

#include <core/concurrency/thread_affinity.h>

#include <ytlib/hydra/hydra_manager.pb.h>

#include <mutex>

namespace NYT {
namespace NHydra {

using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraLogger;

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
//! Returns Null if failed.
template <class TInput>
TErrorOr<TRecordInfo> TryReadRecord(TInput& input)
{
    if (input.Avail() < sizeof(TChangelogRecordHeader)) {
        return TError("Not enough bytes available in data file to read record header: need %v, got %v",
            sizeof(TChangelogRecordHeader),
            input.Avail());
    }

    int readSize = 0;
    TChangelogRecordHeader header;
    readSize += ReadPodPadded(input, header);
    if (!input.Success()) {
        return TError("Error reading record header");
    }
    if (header.DataSize <= 0) {
        return TError("Broken record header: DataSize <= 0");
    }

    struct TSyncChangelogRecordTag { };
    auto data = TSharedMutableRef::Allocate<TSyncChangelogRecordTag>(header.DataSize, false);
    if (input.Avail() < header.DataSize) {
        return TError("Not enough bytes available in data file to read record data: need %v, got %v",
            header.DataSize,
            input.Avail());
    }
    readSize += ReadPadded(input, data);
    if (!input.Success()) {
        return TError("Error reading record data");
    }

    auto checksum = GetChecksum(data);
    if (header.Checksum != checksum) {
        return TError("Record data checksum mismatch of record %v", header.RecordId);
    }

    return TRecordInfo(header.RecordId, readSize);
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

// This method uses forward iterator instead of reverse because they work faster.
// Asserts if last not greater element is absent.
bool CompareRecordIds(const TChangelogIndexRecord& lhs, const TChangelogIndexRecord& rhs)
{
    return lhs.RecordId < rhs.RecordId;
}

bool CompareFilePositions(const TChangelogIndexRecord& lhs, const TChangelogIndexRecord& rhs)
{
    return lhs.FilePosition < rhs.FilePosition;
}

template <class T>
typename std::vector<T>::const_iterator LastNotGreater(
    const std::vector<T>& vec,
    const T& value)
{
    auto res = std::upper_bound(vec.begin(), vec.end(), value);
    YCHECK(res != vec.begin());
    --res;
    return res;
}

template <class T, class TComparator>
typename std::vector<T>::const_iterator LastNotGreater(
    const std::vector<T>& vec,
    const T& value,
    TComparator comparator)
{
    auto res = std::upper_bound(vec.begin(), vec.end(), value, comparator);
    YCHECK(res != vec.begin());
    --res;
    return res;
}

template <class T>
typename std::vector<T>::const_iterator FirstGreater(
    const std::vector<T>& vec,
    const T& value)
{
    return std::upper_bound(vec.begin(), vec.end(), value);
}

template <class T, class TComparator>
typename std::vector<T>::const_iterator FirstGreater(
    const std::vector<T>& vec,
    const T& value,
    TComparator comparator)
{
    return std::upper_bound(vec.begin(), vec.end(), value, comparator);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TSyncFileChangelog::TImpl
{
public:
    TImpl(
        const Stroka& fileName,
        TFileChangelogConfigPtr config)
        : FileName_(fileName)
        , IndexFileName_(fileName + "." + ChangelogIndexExtension)
        , Config_(config)
        , Logger(HydraLogger)
    {
        Logger.AddTag("Path: %v", FileName_);
    }

    TFileChangelogConfigPtr GetConfig() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Config_;
    }

    const Stroka& GetFileName() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return FileName_;
    }


    void Open()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::lock_guard<std::mutex> guard(Mutex_);

        YCHECK(!Open_);

        LOG_DEBUG("Opening changelog");

        DataFile_.reset(new TFileWrapper(FileName_, RdWr|Seq));
        DataFile_->Flock(LOCK_EX | LOCK_NB);

        // Read and check changelog header.
        TChangelogHeader header;
        ReadPod(*DataFile_, header);
        ValidateSignature(header);

        // Read meta.
        auto serializedMeta = TSharedMutableRef::Allocate(header.MetaSize);
        ReadPadded(*DataFile_, serializedMeta);
        DeserializeFromProto(&Meta_, serializedMeta);
        SerializedMeta_ = serializedMeta;

        Open_ = true;
        TruncatedRecordCount_ = header.TruncatedRecordCount == TChangelogHeader::NotTruncatedRecordCount
            ? Null
            : MakeNullable(header.TruncatedRecordCount);

        ReadIndex(header);
        ReadChangelogUntilEnd(header);

        LOG_DEBUG("Changelog opened (RecordCount: %v, Truncated: %v)",
            RecordCount_,
            TruncatedRecordCount_.HasValue());
    }

    void Close()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::lock_guard<std::mutex> guard(Mutex_);

        if (!Open_)
            return;

        DataFile_->FlushData();
        DataFile_->Close();

        IndexFile_->FlushData() ;
        IndexFile_->Close();

        LOG_DEBUG("Changelog closed");

        Open_ = false;
    }

    void Create(const TChangelogMeta& meta)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::lock_guard<std::mutex> guard(Mutex_);

        LOG_DEBUG("Creating changelog");

        YCHECK(!Open_);

        Meta_ = meta;
        SerializedMeta_ = SerializeToProto(Meta_);
        RecordCount_ = 0;
        Open_ = true;

        // Data file.
        i64 currentFilePosition;
        {
            auto tempFileName = FileName_ + NFS::TempFileSuffix;
            TFileWrapper tempFile(tempFileName, WrOnly|CreateAlways);

            TChangelogHeader header(
                SerializedMeta_.Size(),
                TChangelogHeader::NotTruncatedRecordCount);
            WritePod(tempFile, header);

            WritePadded(tempFile, SerializedMeta_);

            currentFilePosition = tempFile.GetPosition();
            YCHECK(currentFilePosition == header.HeaderSize);

            tempFile.FlushData();
            tempFile.Close();

            NFS::Replace(tempFileName, FileName_);

            DataFile_ = std::make_unique<TFileWrapper>(FileName_, RdWr);
            DataFile_->Flock(LOCK_EX | LOCK_NB);
            DataFile_->Seek(0, sEnd);
        }

        // Index file.
        CreateIndexFile();

        CurrentFilePosition_ = currentFilePosition;
        CurrentBlockSize_ = 0;

        LOG_DEBUG("Changelog created");
    }


    const TChangelogMeta& GetMeta() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::lock_guard<std::mutex> guard(Mutex_);
        return Meta_;
    }

    int GetRecordCount() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::lock_guard<std::mutex> guard(Mutex_);
        return RecordCount_;
    }

    i64 GetDataSize() const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::lock_guard<std::mutex> guard(Mutex_);
        return CurrentFilePosition_;
    }

    TInstant GetLastFlushed()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::lock_guard<std::mutex> guard(Mutex_);
        return LastFlushed_;
    }


    void Append(
        int firstRecordId,
        const std::vector<TSharedRef>& records)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::lock_guard<std::mutex> guard(Mutex_);

        LOG_DEBUG("Appending to changelog (RecordIds: %v-%v)",
            firstRecordId,
            firstRecordId + records.size() - 1);

        YCHECK(Open_);
        YCHECK(!TruncatedRecordCount_);
        YCHECK(firstRecordId == RecordCount_);

        // Write records to one blob in memory.
        TBlobOutput memoryOutput;
        int currentRecordCount = RecordCount_;
        std::vector<int> recordSizes;
        for (int i = 0; i < records.size(); ++i) {
            const auto& record = records[i];
            YCHECK(!record.Empty());
            int recordId = currentRecordCount + i;

            int totalSize = 0;
            TChangelogRecordHeader header(recordId, record.Size(), GetChecksum(record));
            totalSize += WritePodPadded(memoryOutput, header);
            totalSize += WritePadded(memoryOutput, record);
            recordSizes.push_back(totalSize);
        }

        // Write blob to file.
        DataFile_->Seek(0, sEnd);
        DataFile_->Write(memoryOutput.Begin(), memoryOutput.Size());

        // Process written records (update index etc).
        for (int i = 0; i < records.size(); ++i) {
            ProcessRecord(currentRecordCount + i, recordSizes[i]);
        }
    }

    void Flush()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::lock_guard<std::mutex> guard(Mutex_);

        LOG_DEBUG("Flushing changelog");

        DataFile_->FlushData();
        IndexFile_->FlushData();
        LastFlushed_ = TInstant::Now();

        LOG_DEBUG("Changelog flushed");
    }

    std::vector<TSharedRef> Read(
        int firstRecordId,
        int maxRecords,
        i64 maxBytes)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::lock_guard<std::mutex> guard(Mutex_);

        // Sanity check.
        YCHECK(firstRecordId >= 0);
        YCHECK(maxRecords >= 0);
        YCHECK(Open_);

        LOG_DEBUG("Reading changelog (FirstRecordId: %v, MaxRecords: %v, MaxBytes: %v)",
            firstRecordId,
            maxRecords,
            maxBytes);

        std::vector<TSharedRef> records;

        // Prevent search in empty index.
        if (Index_.empty()) {
            return std::move(records);
        }

        maxRecords = std::min(maxRecords, RecordCount_ - firstRecordId);
        int lastRecordId = firstRecordId + maxRecords; // non-inclusive

        // Read envelope piece of changelog.
        auto envelope = ReadEnvelope(firstRecordId, lastRecordId, std::min(Index_.back().FilePosition, maxBytes));

        // Read records from envelope data and save them to the records.
        i64 readBytes = 0;
        TMemoryInput inputStream(envelope.Blob.Begin(), envelope.GetLength());
        for (int recordId = envelope.GetStartRecordId();
             recordId < envelope.GetEndRecordId() && recordId < lastRecordId && readBytes < maxBytes;
             ++recordId)
        {
            // Read and check header.
            TChangelogRecordHeader header;
            ReadPodPadded(inputStream, header);
            YCHECK(header.RecordId == recordId);

            // Save and pad data.
            i64 startOffset = inputStream.Buf() - envelope.Blob.Begin();
            i64 endOffset = startOffset + header.DataSize;
            auto data = envelope.Blob.Slice(startOffset, endOffset);
            inputStream.Skip(AlignUp(header.DataSize));

            // Add data to the records.
            if (recordId >= firstRecordId) {
                records.push_back(data);
                readBytes += data.Size();
            }
        }

        return records;
    }

    void Truncate(int recordCount)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        std::lock_guard<std::mutex> guard(Mutex_);

        YCHECK(Open_);
        YCHECK(recordCount >= 0);
        YCHECK(!TruncatedRecordCount_ || recordCount <= *TruncatedRecordCount_);

        RecordCount_ = recordCount;
        TruncatedRecordCount_ = recordCount;

        UpdateLogHeader();

        LOG_DEBUG("Changelog truncated (RecordCount: %v)",
            recordCount);
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


    //! Creates an empty index file.
    void CreateIndexFile()
    {
        auto tempFileName = IndexFileName_ + NFS::TempFileSuffix;
        TFile tempFile(tempFileName, WrOnly|CreateAlways);

        TChangelogIndexHeader header(0);
        WritePod(tempFile, header);

        tempFile.FlushData();
        tempFile.Close();

        NFS::Replace(tempFileName, IndexFileName_);

        IndexFile_ = std::make_unique<TFile>(IndexFileName_, RdWr);
        IndexFile_->Flock(LOCK_EX | LOCK_NB);
        IndexFile_->Seek(0, sEnd);
    }

    //! Processes records that are being or written.
    /*!
     *  Checks record id for correctness, updates index, record count,
     *  current block size and current file position.
     */
    void ProcessRecord(int recordId, int readSize)
    {
        YCHECK(RecordCount_ == recordId);

        // We add a new index record
        // 1) for the very first data record; or
        // 2) if the size of data records added since last index record exceeds IndexBlockSize.
        if (RecordCount_ == 0 || CurrentBlockSize_ >= Config_->IndexBlockSize) {
            YCHECK(Index_.empty() || Index_.back().RecordId < recordId);

            CurrentBlockSize_ = 0;
            Index_.push_back(TChangelogIndexRecord(recordId, CurrentFilePosition_));
            WritePod(*IndexFile_, Index_.back());
            UpdateIndexHeader();

            LOG_DEBUG("Changelog index record added (RecordId: %v, Offset: %v)",
                recordId,
                CurrentFilePosition_);
        }
        // Record appended successfully.
        CurrentBlockSize_ += readSize;
        CurrentFilePosition_ += readSize;
        RecordCount_ += 1;
    }

    //! Rewrites changelog header.
    void UpdateLogHeader()
    {
        DataFile_->FlushData();
        i64 oldPosition = DataFile_->GetPosition();
        DataFile_->Seek(0, sSet);
        TChangelogHeader header(
            SerializedMeta_.Size(),
            TruncatedRecordCount_ ? *TruncatedRecordCount_ : TChangelogHeader::NotTruncatedRecordCount);
        WritePod(*DataFile_, header);
        DataFile_->FlushData();
        DataFile_->Seek(oldPosition, sSet);
    }

    //! Rewrites index header.
    void UpdateIndexHeader()
    {
        IndexFile_->FlushData();
        i64 oldPosition = IndexFile_->GetPosition();
        IndexFile_->Seek(0, sSet);
        TChangelogIndexHeader header(Index_.size());
        WritePod(*IndexFile_, header);
        IndexFile_->Seek(oldPosition, sSet);
    }

    //! Reads the maximal valid prefix of index, truncates bad index records.
    void ReadIndex(const TChangelogHeader& header)
    {
        // Create index if it is missing.
        if (!NFS::Exists(IndexFileName_) ||
            TFile(IndexFileName_, RdOnly).GetLength() < sizeof(TChangelogIndexHeader))
        {
            CreateIndexFile();
        }

        // Read the existing index.
        {
            TMappedFileInput indexStream(IndexFileName_);

            // Read and check index header.
            TChangelogIndexHeader indexHeader;
            ReadPod(indexStream, indexHeader);
            ValidateSignature(indexHeader);
            YCHECK(indexHeader.IndexRecordCount >= 0);

            // Read index records.
            for (int i = 0; i < indexHeader.IndexRecordCount; ++i) {
                if (indexStream.Avail() < sizeof(TChangelogIndexHeader)) {
                    break;
                }

                TChangelogIndexRecord indexRecord;
                ReadPod(indexStream, indexRecord);
                if (TruncatedRecordCount_ && indexRecord.RecordId >= *TruncatedRecordCount_) {
                    break;
                }
                Index_.push_back(indexRecord);
            }
        }
        // Compute the maximum correct prefix and truncate the index.
        {
            auto correctPrefixSize = ComputeValidIndexPrefix(Index_, header, &*DataFile_);
            LOG_ERROR_IF(correctPrefixSize < Index_.size(), "Changelog index contains invalid records, truncated");
            Index_.resize(correctPrefixSize);

            IndexFile_.reset(new TFile(IndexFileName_, RdWr|Seq|CloseOnExec|OpenAlways));
            IndexFile_->Flock(LOCK_EX | LOCK_NB);
            IndexFile_->Resize(sizeof(TChangelogIndexHeader) + Index_.size() * sizeof(TChangelogIndexRecord));
            IndexFile_->Seek(0, sEnd);
        }
    }

    //! Reads a piece of changelog containing both #firstRecordId and #lastRecordId.
    TEnvelopeData ReadEnvelope(int firstRecordId, int lastRecordId, i64 maxBytes = -1)
    {
        YCHECK(!Index_.empty());

        TEnvelopeData result;
        result.LowerBound = *LastNotGreater(Index_, TChangelogIndexRecord(firstRecordId, -1), CompareRecordIds);

        auto it = FirstGreater(Index_, TChangelogIndexRecord(lastRecordId, -1), CompareRecordIds);
        if (maxBytes != -1) {
            i64 maxFilePosition = result.LowerBound.FilePosition + maxBytes;
            it = std::min(it, FirstGreater(Index_, TChangelogIndexRecord(-1, maxFilePosition), CompareFilePositions));
        }
        result.UpperBound =
            it != Index_.end() ?
                *it :
                TChangelogIndexRecord(RecordCount_, CurrentFilePosition_);

        struct TSyncChangelogEnvelopeTag { };
        result.Blob = TSharedMutableRef::Allocate<TSyncChangelogEnvelopeTag>(result.GetLength(), false);

        size_t bytesRead = DataFile_->Pread(
            result.Blob.Begin(),
            result.GetLength(),
            result.GetStartPosition());
        YCHECK(bytesRead == result.GetLength());

        return result;
    }

    //! Reads changelog starting from the last indexed record until the end of file.
    void ReadChangelogUntilEnd(const TChangelogHeader& header)
    {
        // Extract changelog properties from index.
        i64 fileLength = DataFile_->GetLength();
        CurrentBlockSize_ = 0;
        if (Index_.empty()) {
            RecordCount_ = 0;
            CurrentFilePosition_ = header.HeaderSize;
        } else {
            // Record count would be set below.
            CurrentFilePosition_ = Index_.back().FilePosition;
        }

        // Seek to proper position in file, initialize checkable reader.
        DataFile_->Seek(CurrentFilePosition_, sSet);
        TCheckedReader<TFileWrapper> dataReader(*DataFile_);

        if (!Index_.empty()) {
            // Skip the first index record.
            // It must be correct since we have already checked the index.
            auto recordInfoOrError = TryReadRecord(dataReader);
            YCHECK(recordInfoOrError.IsOK());
            const auto& recordInfo = recordInfoOrError.Value();
            RecordCount_ = Index_.back().RecordId + 1;
            CurrentFilePosition_ += recordInfo.TotalSize;
        }

        while (CurrentFilePosition_ < fileLength) {
            auto recordInfoOrError = TryReadRecord(dataReader);
            if (!recordInfoOrError.IsOK()) {
                if (TruncatedRecordCount_ && RecordCount_ < *TruncatedRecordCount_) {
                    THROW_ERROR_EXCEPTION("Broken record found in truncated changelog %v",
                        FileName_)
                        << TErrorAttribute("record_id", RecordCount_)
                        << TErrorAttribute("offset", CurrentFilePosition_);
                }

                DataFile_->Resize(CurrentFilePosition_);
                DataFile_->FlushData();
                DataFile_->Seek(0, sEnd);

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

            if (TruncatedRecordCount_ && RecordCount_ == *TruncatedRecordCount_) {
                break;
            }

            ProcessRecord(recordInfoOrError.Value().Id, recordInfoOrError.Value().TotalSize);
        }
    }


    const Stroka FileName_;
    const Stroka IndexFileName_;
    const TFileChangelogConfigPtr Config_;

    bool Open_ = false;
    int RecordCount_ = -1;
    TNullable<int> TruncatedRecordCount_;
    i64 CurrentBlockSize_ = -1;
    i64 CurrentFilePosition_ = -1;
    TInstant LastFlushed_;

    TChangelogMeta Meta_;
    TSharedRef SerializedMeta_;

    std::vector<TChangelogIndexRecord> Index_;

    std::unique_ptr<TFileWrapper> DataFile_;
    std::unique_ptr<TFile> IndexFile_;

    //! Auxiliary data.
    //! Protects file resources.
    mutable std::mutex Mutex_;
    NLogging::TLogger Logger;

};

////////////////////////////////////////////////////////////////////////////////

TSyncFileChangelog::TSyncFileChangelog(
    const Stroka& fileName,
    TFileChangelogConfigPtr config)
    : Impl_(new TImpl(
        fileName,
        config))
{ }

TSyncFileChangelog::~TSyncFileChangelog()
{ }

TFileChangelogConfigPtr TSyncFileChangelog::GetConfig()
{
    return Impl_->GetConfig();
}

const Stroka& TSyncFileChangelog::GetFileName() const
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

TInstant TSyncFileChangelog::GetLastFlushed()
{
    return Impl_->GetLastFlushed();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
