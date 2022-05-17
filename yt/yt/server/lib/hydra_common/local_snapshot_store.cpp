#include "private.h"
#include "local_snapshot_store.h"

#include <yt/yt/server/lib/hydra_common/config.h>
#include <yt/yt/server/lib/hydra_common/file_helpers.h>
#include <yt/yt/server/lib/hydra_common/format.h>
#include <yt/yt/server/lib/hydra_common/snapshot.h>
#include <yt/yt/server/lib/hydra_common/private.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/streams/lz/lz.h>

#include <util/stream/file.h>

namespace NYT::NHydra {

using namespace NFS;
using namespace NCompression;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TLocalSnapshotReader)
DECLARE_REFCOUNTED_CLASS(TRawLocalSnapshotReader)
DECLARE_REFCOUNTED_CLASS(TLocalSnapshotWriter)

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 ReaderBlockSize = 1_MB;

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TLocalSnapshotReaderBase
    : public ISnapshotReader
{
public:
    TLocalSnapshotReaderBase(
        const TString& fileName,
        int snapshotId,
        NLogging::TLogger logger)
        : FileName_(fileName)
        , SnapshotId_(snapshotId)
        , Logger(std::move(logger))
    { }

    int GetSnapshotId() const
    {
        return SnapshotId_;
    }

    TFuture<void> Open() override
    {
        return BIND(&TLocalSnapshotReaderBase::DoOpen, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run();
    }

    TFuture<TSharedRef> Read() override
    {
        return BIND(&TLocalSnapshotReaderBase::DoRead, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run();
    }

    TSnapshotParams GetParams() const override
    {
        return TSnapshotParams{
            .Meta = Meta_,
            .Checksum = Header_.Checksum,
            .CompressedLength = static_cast<i64>(Header_.CompressedLength),
            .UncompressedLength = static_cast<i64>(Header_.UncompressedLength)
        };
    }

private:
    const TString FileName_;
    const int SnapshotId_;

    const NLogging::TLogger Logger;

    std::unique_ptr<TFile> File_;
    std::unique_ptr<TUnbufferedFileInput> FileInput_;
    IInputStream* FacadeInput_;

    TSnapshotHeader Header_;
    TSnapshotMeta Meta_;

    TImpl* CrtpThis()
    {
        return static_cast<TImpl*>(this);
    }

    void DoOpen()
    {
        YT_LOG_DEBUG("Opening local snapshot reader");

        try {
            TFileHandle fileHandle(FileName_, RdOnly | OpenExisting | CloseOnExec);
            if (!fileHandle.IsOpen()) {
                THROW_ERROR_EXCEPTION(EErrorCode::NoSuchSnapshot, "Failed to open snapshot file");
            }
            File_.reset(new TFile(fileHandle.Release(), FileName_));

            TUnbufferedFileInput input(*File_);

            ui64 signature;
            ReadPod(input, signature);

            if (signature != TSnapshotHeader::ExpectedSignature) {
                THROW_ERROR_EXCEPTION("Unrecognized snapshot signature %" PRIx64,
                    signature);
            }

            File_->Seek(0, sSet);
            ReadPod(input, Header_);

            if (Header_.SnapshotId != SnapshotId_ && SnapshotId_ != InvalidSegmentId) {
                THROW_ERROR_EXCEPTION(
                    "Invalid snapshot id in header of %v: expected %v, got %v",
                    FileName_,
                    SnapshotId_,
                    Header_.SnapshotId);
            }

            if (static_cast<i64>(Header_.CompressedLength) != File_->GetLength()) {
                THROW_ERROR_EXCEPTION(
                    "Invalid compressed length in header of %v: expected %v, got %v",
                    FileName_,
                    File_->GetLength(),
                    Header_.CompressedLength);
            }

            auto serializedMeta = TSharedMutableRef::Allocate(Header_.MetaSize, /* initializeStorage */ false);
            ReadRefPadded(input, serializedMeta);
            DeserializeProto(&Meta_, serializedMeta);

            CrtpThis()->SeekFile(File_.get());

            FileInput_.reset(new TUnbufferedFileInput(*File_));

            FacadeInput_ = CrtpThis()->WrapFileInput(FileInput_.get(), Header_.Codec);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error opening snapshot %v for reading",
                FileName_)
                << ex;
        }

        YT_LOG_DEBUG("Local snapshot reader opened");
    }

    TSharedRef DoRead()
    {
        auto block = TSharedMutableRef::Allocate(ReaderBlockSize, /* initializeStorage */ false);
        size_t length = FacadeInput_->Load(block.Begin(), block.Size());
        return length == 0 ? TSharedRef() : block.Slice(0, length);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRawLocalSnapshotReader
    : public TLocalSnapshotReaderBase<TRawLocalSnapshotReader>
{
public:
    TRawLocalSnapshotReader(
        const TString& fileName,
        int snapshotId,
        i64 offset)
        : TLocalSnapshotReaderBase(
            fileName,
            snapshotId,
            HydraLogger.WithTag("Path: %v, Raw: true, Offset: %v", fileName, offset))
        , Offset_(offset)
    { }

    void SeekFile(TFile* file)
    {
        file->Seek(Offset_, sSet);
    }

    IInputStream* WrapFileInput(TUnbufferedFileInput* fileInput, NCompression::ECodec)
    {
        return fileInput;
    }

private:
    const i64 Offset_;
};

DEFINE_REFCOUNTED_TYPE(TRawLocalSnapshotReader)

////////////////////////////////////////////////////////////////////////////////

class TLocalSnapshotReader
    : public TLocalSnapshotReaderBase<TLocalSnapshotReader>
{
public:
    TLocalSnapshotReader(
        const TString& fileName,
        int snapshotId)
        : TLocalSnapshotReaderBase(
            fileName,
            snapshotId,
            HydraLogger.WithTag("Path: %v, Raw: false", fileName))
    { }

    void SeekFile(TFile*)
    { }

    IInputStream* WrapFileInput(TUnbufferedFileInput* fileInput, NCompression::ECodec codec)
    {
        switch (codec) {
            case ECodec::None:
                return fileInput;
            case ECodec::Snappy:
                CodecInput_.reset(new TSnappyDecompress(fileInput));
                return CodecInput_.get();
            case ECodec::Lz4:
                CodecInput_.reset(new TLz4Decompress(fileInput));
                return CodecInput_.get();
            default:
                YT_ABORT();
        }
    }

private:
    std::unique_ptr<IInputStream> CodecInput_;
};

DEFINE_REFCOUNTED_TYPE(TLocalSnapshotReader)

////////////////////////////////////////////////////////////////////////////////

ISnapshotReaderPtr CreateLocalSnapshotReader(const TString& fileName, int snapshotId)
{
    return New<TLocalSnapshotReader>(fileName, snapshotId);
}

////////////////////////////////////////////////////////////////////////////////

class TLocalSnapshotWriter
    : public ISnapshotWriter
{
public:
    TLocalSnapshotWriter(
        const TString& fileName,
        ECodec codec,
        int snapshotId,
        const TSnapshotMeta& meta,
        bool raw)
        : FileName_(fileName)
        , Codec_(codec)
        , SnapshotId_(snapshotId)
        , Meta_(meta)
        , IsRaw_(raw)
    {
        SerializedMeta_ = SerializeProtoToRef(Meta_);
        Logger.AddTag("Path: %v", FileName_);
    }

    ~TLocalSnapshotWriter()
    {
        // TODO(babenko): consider moving this code into HydraIO queue
        if (!IsClosed_) {
            try {
                DoFinish();
                File_->Close();
                NFS::Remove(FileName_ + TempFileSuffix);
            } catch (const std::exception& ex) {
                YT_LOG_WARNING(ex, "Error removing temporary local snapshot, ignored (FileName: %v)",
                    FileName_ + TempFileSuffix);
            }
        }
    }

    TFuture<void> Open() override
    {
        return BIND(&TLocalSnapshotWriter::DoOpen, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run();
    }

    TFuture<void> Write(const TSharedRef& buffer) override
    {
        return BIND(&TLocalSnapshotWriter::DoWrite, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run(buffer);
    }

    TFuture<void> Close() override
    {
        return BIND(&TLocalSnapshotWriter::DoClose, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run();
    }

    TSnapshotParams GetParams() const override
    {
        YT_VERIFY(IsClosed_);
        return Params_;
    }

    DEFINE_SIGNAL(void(), Closed);

private:
    const TString FileName_;
    const ECodec Codec_;
    const int SnapshotId_;
    const TSnapshotMeta Meta_;
    const bool IsRaw_;

    TSharedRef SerializedMeta_;

    bool IsOpened_ = false;
    bool IsClosed_ = false;
    TSnapshotParams Params_;

    std::unique_ptr<TFile> File_;
    std::unique_ptr<TUnbufferedFileOutput> FileOutput_;
    std::unique_ptr<IOutputStream> CodecOutput_;
    std::unique_ptr<TChecksumOutput> ChecksumOutput_;
    std::unique_ptr<TLengthMeasureOutputStream> LengthMeasureOutput_;
    IOutputStream* FacadeOutput_ = nullptr;

    NLogging::TLogger Logger = HydraLogger;


    void DoWrite(const TSharedRef& buffer)
    {
        YT_VERIFY(IsOpened_ && !IsClosed_);
        FacadeOutput_->Write(buffer.Begin(), buffer.Size());
    }

    void DoOpen()
    {
        YT_VERIFY(!IsOpened_);

        YT_LOG_DEBUG("Opening local snapshot writer (Codec: %v, Raw: %v)",
            Codec_,
            IsRaw_);

        try {
            File_.reset(new TFile(FileName_ + TempFileSuffix, CreateAlways | CloseOnExec));
            FileOutput_.reset(new TUnbufferedFileOutput(*File_));

            if (IsRaw_) {
                FacadeOutput_ = FileOutput_.get();
            } else {
                TSnapshotHeader header;
                Zero(header);
                WritePod(*File_, header);
                WriteRefPadded(*File_, SerializedMeta_);
                File_->Flush();

                ChecksumOutput_.reset(new TChecksumOutput(FileOutput_.get()));
                switch (Codec_) {
                    case ECodec::None:
                        break;
                    case ECodec::Snappy:
                        CodecOutput_.reset(new TSnappyCompress(ChecksumOutput_.get()));
                        break;
                    case ECodec::Lz4:
                        CodecOutput_.reset(new TLz4Compress(ChecksumOutput_.get()));
                        break;
                    default:
                        YT_ABORT();
                }
                LengthMeasureOutput_.reset(new TLengthMeasureOutputStream(CodecOutput_
                    ? CodecOutput_.get()
                    : ChecksumOutput_.get()));
                FacadeOutput_ = LengthMeasureOutput_.get();
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error opening snapshot %v for writing",
                FileName_)
                << ex;
        }

        IsOpened_ = true;

        YT_LOG_DEBUG("Local snapshot writer opened");
    }

    void DoFinish()
    {
        // NB: Some calls might be redundant.
        if (FacadeOutput_) {
            FacadeOutput_->Finish();
        }
        if (LengthMeasureOutput_) {
            LengthMeasureOutput_->Finish();
        }
        if (CodecOutput_) {
            CodecOutput_->Finish();
        }
        if (ChecksumOutput_) {
            ChecksumOutput_->Finish();
        }
        if (FileOutput_) {
            FileOutput_->Finish();
        }
    }

    void DoClose()
    {
        YT_VERIFY(IsOpened_ && !IsClosed_);

        YT_LOG_DEBUG("Closing local snapshot writer");

        DoFinish();

        Params_.Meta = Meta_;
        if (ChecksumOutput_) {
            Params_.Checksum = ChecksumOutput_->GetChecksum();
        }
        Params_.CompressedLength = File_->GetLength();
        if (LengthMeasureOutput_) {
            Params_.UncompressedLength = LengthMeasureOutput_->GetLength();
        }

        if (!IsRaw_) {
            TSnapshotHeader header;
            Zero(header);
            header.Signature = TSnapshotHeader::ExpectedSignature;
            header.SnapshotId = SnapshotId_;
            header.CompressedLength = Params_.CompressedLength;
            header.UncompressedLength = Params_.UncompressedLength;
            header.Checksum = Params_.Checksum;
            header.Codec = Codec_;
            header.MetaSize = SerializedMeta_.Size();
            File_->Seek(0, sSet);
            WritePod(*File_, header);
        }

        File_->Flush();
        File_->Close();

        NFS::Rename(FileName_ + TempFileSuffix, FileName_);

        Closed_.Fire();

        IsClosed_ = true;

        YT_LOG_DEBUG("Local snapshot writer closed");
    }
};

DEFINE_REFCOUNTED_TYPE(TLocalSnapshotWriter)

////////////////////////////////////////////////////////////////////////////////

class TLocalSystemSnapshotStore
    : public ILegacySnapshotStore
{
public:
    explicit TLocalSystemSnapshotStore(TLocalSnapshotStoreConfigPtr config)
        : Config_(std::move(config))
        , Logger(HydraLogger.WithTag("Path: %v", Config_->Path))
    {
        auto path = Config_->Path;

        YT_LOG_INFO("Preparing snapshot directory");

        NFS::MakeDirRecursive(path);
        NFS::CleanTempFiles(path);

        YT_LOG_INFO("Snapshot scan started");

        auto fileNames = EnumerateFiles(path);
        for (const auto& fileName : fileNames) {
            auto extension = NFS::GetFileExtension(fileName);
            if (extension != SnapshotExtension) {
                continue;
            }

            auto name = NFS::GetFileNameWithoutExtension(fileName);

            int snapshotId;
            if (!TryFromString(name, snapshotId)) {
                YT_LOG_WARNING("Found unrecognized file in snapshot directory (FileName: %v)",
                    fileName);
                continue;
            }

            RegisterSnapshot(snapshotId);
        }

        YT_LOG_INFO("Snapshot scan completed");
    }

    TFuture<int> GetLatestSnapshotId(int maxSnapshotId) override
    {
        auto guard = Guard(SpinLock_);

        auto it = RegisteredSnapshotIds_.upper_bound(maxSnapshotId);
        if (it == RegisteredSnapshotIds_.begin()) {
            return MakeFuture(InvalidSegmentId);
        }

        int snapshotId = *(--it);
        YT_VERIFY(snapshotId <= maxSnapshotId);
        return MakeFuture(snapshotId);
    }

    ISnapshotReaderPtr CreateReader(int snapshotId) override
    {
        if (!CheckSnapshotExists(snapshotId)) {
            THROW_ERROR_EXCEPTION(EErrorCode::NoSuchSnapshot, "No such snapshot %v", snapshotId);
        }

        return CreateLocalSnapshotReader(
            GetSnapshotPath(snapshotId),
            snapshotId);
    }

    ISnapshotReaderPtr CreateRawReader(int snapshotId, i64 offset) override
    {
        return New<TRawLocalSnapshotReader>(
            GetSnapshotPath(snapshotId),
            snapshotId,
            offset);
    }

    ISnapshotWriterPtr CreateWriter(int snapshotId, const TSnapshotMeta& meta) override
    {
        return DoCreateWriter(
            snapshotId,
            GetSnapshotPath(snapshotId),
            Config_->Codec,
            snapshotId,
            meta,
            /*raw*/ false);
    }

    ISnapshotWriterPtr CreateRawWriter(int snapshotId) override
    {
        return DoCreateWriter(
            snapshotId,
            GetSnapshotPath(snapshotId),
            Config_->Codec,
            snapshotId,
            TSnapshotMeta(),
            /*raw*/ true);
    }

private:
    const TLocalSnapshotStoreConfigPtr Config_;
    const NLogging::TLogger Logger;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    std::set<int> RegisteredSnapshotIds_;
    THashMap<int, TWeakPtr<TLocalSnapshotWriter>> SnapshotIdToWriter_;


    TString GetSnapshotPath(int snapshotId)
    {
        return NFS::CombinePaths(
            Config_->Path,
            Format("%09d.%v", snapshotId, SnapshotExtension));
    }

    bool CheckSnapshotExists(int snapshotId)
    {
        if (NFS::Exists(GetSnapshotPath(snapshotId))) {
            return true;
        }

        {
            auto guard = Guard(SpinLock_);
            if (RegisteredSnapshotIds_.erase(snapshotId) == 1) {
                YT_LOG_WARNING("Erased orphaned snapshot from store (SnapshotId: %v)",
                    snapshotId);
            }
        }

        return false;
    }

    template <class... TArgs>
    ISnapshotWriterPtr DoCreateWriter(int snapshotId, TArgs&&... args)
    {
        bool snapshotExists = NFS::Exists(GetSnapshotPath(snapshotId));

        auto guard = Guard(SpinLock_);

        if (!snapshotExists && RegisteredSnapshotIds_.erase(snapshotId) > 0) {
            YT_LOG_WARNING("Erased orphaned snapshot from store (SnapshotId: %v)",
                snapshotId);
        }

        if (RegisteredSnapshotIds_.contains(snapshotId)) {
            THROW_ERROR_EXCEPTION("Snapshot %v is already present in store",
                snapshotId);
        }

        // Scan through registered writers dropping expired ones and checking for conflicts.
        for (auto it = SnapshotIdToWriter_.begin(); it != SnapshotIdToWriter_.end();) {
            if (it->second.IsExpired()) {
                SnapshotIdToWriter_.erase(it++);
                continue;
            }

            if (it->first == snapshotId) {
                THROW_ERROR_EXCEPTION("Snapshot %v is already being written",
                    snapshotId);
            }

            ++it;
        }

        auto writer = New<TLocalSnapshotWriter>(std::forward<TArgs>(args)...);
        writer->SubscribeClosed(BIND(
            &TLocalSystemSnapshotStore::OnWriterClosed,
            MakeWeak(this),
            snapshotId));

        SnapshotIdToWriter_[snapshotId] = writer;

        return writer;
    }

    void OnWriterClosed(int snapshotId)
    {
        auto guard = Guard(SpinLock_);

        EraseOrCrash(SnapshotIdToWriter_, snapshotId);
        InsertOrCrash(RegisteredSnapshotIds_, snapshotId);

        YT_LOG_INFO("Snapshot registered (SnapshotId: %v)",
            snapshotId);
    }

    void RegisterSnapshot(int snapshotId)
    {
        auto guard = Guard(SpinLock_);

        InsertOrCrash(RegisteredSnapshotIds_, snapshotId);

        YT_LOG_INFO("Snapshot registered (SnapshotId: %v)",
            snapshotId);
    }
};

////////////////////////////////////////////////////////////////////////////////

ILegacySnapshotStorePtr CreateLocalSnapshotStore(TLocalSnapshotStoreConfigPtr config)
{
    return New<TLocalSystemSnapshotStore>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

class TUncompressedHeaderlessLocalSnapshotReader
    : public ISnapshotReader
{
public:
    TUncompressedHeaderlessLocalSnapshotReader(const TString& fileName)
        : FileName_(fileName)
        , Logger(HydraLogger.WithTag("Path: %v", FileName_))
    { }

    TFuture<void> Open() override
    {
        return BIND(&TUncompressedHeaderlessLocalSnapshotReader::DoOpen, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run();
    }

    TFuture<TSharedRef> Read() override
    {
        return BIND(&TUncompressedHeaderlessLocalSnapshotReader::DoRead, MakeStrong(this))
            .AsyncVia(GetHydraIOInvoker())
            .Run();
    }

    TSnapshotParams GetParams() const override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const TString FileName_;
    const NLogging::TLogger Logger;

    std::unique_ptr<TFile> File_;
    std::unique_ptr<TUnbufferedFileInput> FileInput_;

    void DoOpen()
    {
        YT_LOG_DEBUG("Opening uncompressed headerless file snapshot reader (FileName: %v)",
            FileName_);

        try {
            TFileHandle fileHandle(FileName_, RdOnly | OpenExisting | CloseOnExec);
            if (!fileHandle.IsOpen()) {
                THROW_ERROR_EXCEPTION(EErrorCode::NoSuchSnapshot, "Failed to open snapshot file");
            }
            File_.reset(new TFile(fileHandle.Release(), FileName_));

            FileInput_.reset(new TUnbufferedFileInput(*File_));

            YT_LOG_DEBUG("Uncompressed headerless file snapshot reader opened (FileName: %v)",
                FileName_);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error opening snapshot %v for reading",
                FileName_)
                << ex;
        }

        YT_LOG_DEBUG("Local snapshot reader opened");
    }

    TSharedRef DoRead()
    {
        auto block = TSharedMutableRef::Allocate(ReaderBlockSize, false);
        size_t length = FileInput_->Load(block.Begin(), block.Size());
        return length == 0 ? TSharedRef() : block.Slice(0, length);
    }
};

DEFINE_REFCOUNTED_TYPE(TUncompressedHeaderlessLocalSnapshotReader)

////////////////////////////////////////////////////////////////////////////////

ISnapshotReaderPtr CreateUncompressedHeaderlessLocalSnapshotReader(const TString& fileName)
{
    return New<TUncompressedHeaderlessLocalSnapshotReader>(fileName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
