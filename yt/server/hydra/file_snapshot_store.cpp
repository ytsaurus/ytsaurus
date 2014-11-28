#include "stdafx.h"
#include "file_snapshot_store.h"
#include "snapshot.h"
#include "config.h"
#include "file_helpers.h"
#include "format.h"
#include "private.h"

#include <core/misc/fs.h>
#include <core/misc/serialize.h>
#include <core/misc/checksum.h>
#include <core/misc/checkpointable_stream.h>

#include <core/logging/log.h>

#include <ytlib/hydra/hydra_manager.pb.h>

#include <util/stream/lz.h>
#include <util/stream/file.h>
#include <util/stream/buffered.h>

namespace NYT {
namespace NHydra {

using namespace NFS;
using namespace NCompression;
using namespace NHydra::NProto;

////////////////////////////////////////////////////////////////////////////////

class TFileSnapshotReader
    : public ISnapshotReader
{
public:
    TFileSnapshotReader(
        const Stroka& fileName,
        int snapshotId,
        bool isRaw)
        : FileName_(fileName)
        , SnapshotId_(snapshotId)
        , IsRaw_(isRaw)
        , Logger(HydraLogger)
    {
        Logger.AddTag("FileName: %v", FileName_);
    }

    void Open(i64 offset)
    {
        try {
            File_.reset(new TFile(FileName_, OpenExisting | CloseOnExec));
            TFileInput input(*File_);

            ui64 signature;
            ReadPod(input, signature);
            File_->Seek(0, sSet);

            if (signature == TSnapshotHeader::ExpectedSignature) {
                ReadPod(input, Header_);

                if (Header_.SnapshotId != SnapshotId_) {
                    THROW_ERROR_EXCEPTION(
                        "Invalid snapshot id in header of %v: expected %v, got %v",
                        FileName_,
                        SnapshotId_,
                        Header_.SnapshotId);
                }

                if (Header_.CompressedLength != File_->GetLength()) {
                    THROW_ERROR_EXCEPTION(
                        "Invalid compressed length in header of %v: expected %v, got %v",
                        FileName_,
                        File_->GetLength(),
                        Header_.CompressedLength);
                }

                Meta_ = TSharedRef::Allocate(Header_.MetaSize, false);
                ReadPadded(input, Meta_);

                if (IsRaw_) {
                    File_->Seek(offset, sSet);
                }

                FileInput_.reset(new TBufferedFileInput(*File_));

                auto codec = ECodec(Header_.Codec);
                if (IsRaw_) {
                    FacadeInput_ = FileInput_.get();
                } else {
                    switch (codec) {
                        case ECodec::None:
                            break;
                        case ECodec::Snappy:
                            CodecInput_.reset(new TSnappyDecompress(FileInput_.get()));
                            break;
                        case ECodec::Lz4:
                            CodecInput_.reset(new TLz4Decompress(FileInput_.get()));
                            break;
                        default:
                            YUNREACHABLE();
                    }
                    FacadeInput_ = CodecInput_ ? CodecInput_.get() : FileInput_.get();
                }
            } else if (signature == TSnapshotHeader_0_16::ExpectedSignature) {
                TSnapshotHeader_0_16 legacyHeader;
                ReadPod(input, legacyHeader);

                Header_.SnapshotId = legacyHeader.SnapshotId;
                if (Header_.SnapshotId != SnapshotId_) {
                    THROW_ERROR_EXCEPTION(
                        "Invalid snapshot id in header of %v: expected %v, got %v",
                        FileName_,
                        SnapshotId_,
                        Header_.SnapshotId);
                }

                Header_.CompressedLength = legacyHeader.DataLength + sizeof (legacyHeader);
                if (Header_.CompressedLength != File_->GetLength()) {
                    THROW_ERROR_EXCEPTION(
                        "Invalid compressed length in header of %v: expected %v, got %v",
                        FileName_,
                        File_->GetLength(),
                        Header_.CompressedLength);
                }

                TSnapshotMeta meta;
                meta.set_prev_record_count(legacyHeader.PrevRecordCount);
                YCHECK(SerializeToProto(meta, &Meta_));

                Header_.Checksum = legacyHeader.Checksum;
                Header_.UncompressedLength = Header_.CompressedLength;
                Header_.Codec = ECodec::Snappy;
                Header_.MetaSize = Meta_.Size();

                if (IsRaw_) {
                    File_->Seek(offset, sSet);
                }

                FileInput_.reset(new TBufferedFileInput(*File_));

                if (IsRaw_) {
                    FacadeInput_ = FileInput_.get();
                } else {
                    CodecInput_.reset(new TSnappyDecompress(FileInput_.get()));
                    FakeCheckpointableInput_ = EscapsulateAsCheckpointableInputStream(CodecInput_.get());
                    FacadeInput_ = FakeCheckpointableInput_.get();
                }
            } else {
                THROW_ERROR_EXCEPTION("Unrecognized snapshot signature %" PRIx64,
                    signature);
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error opening snapshot %v for reading",
                FileName_)
                << ex;
        }
    }

    virtual TInputStream* GetStream() override
    {
        return FacadeInput_;
    }

    virtual TSnapshotParams GetParams() const override
    {
        TSnapshotParams params;
        params.Meta = Meta_;
        params.Checksum = Header_.Checksum;
        params.CompressedLength = Header_.CompressedLength;
        params.UncompressedLength = Header_.UncompressedLength;
        return params;
    }

private:
    Stroka FileName_;
    int SnapshotId_;
    bool IsRaw_;

    NLog::TLogger Logger;

    std::unique_ptr<TFile> File_;
    std::unique_ptr<TBufferedFileInput> FileInput_;
    std::unique_ptr<TInputStream> CodecInput_;
    std::unique_ptr<TInputStream> FakeCheckpointableInput_;
    TInputStream* FacadeInput_;

    TSnapshotHeader Header_;
    TSharedRef Meta_;

};

ISnapshotReaderPtr CreateFileSnapshotReader(
    const Stroka& fileName,
    int snapshotId,
    bool isRaw,
    i64 offset)
{
    auto reader = New<TFileSnapshotReader>(
        fileName,
        snapshotId,
        isRaw);
    reader->Open(offset);
    return reader;
}

////////////////////////////////////////////////////////////////////////////////

class TFileSnapshotWriter
    : public ISnapshotWriter
{
public:
    TFileSnapshotWriter(
        const Stroka& fileName,
        ECodec codec,
        int snapshotId,
        const TSharedRef& meta,
        bool isRaw)
        : FileName_(fileName)
        , Codec_(codec)
        , SnapshotId_(snapshotId)
        , Meta_(meta)
        , IsRaw_(isRaw)
        , CheckpointableOutput_(nullptr)
    { }

    void Open()
    {
        // NB: Avoid logging here, this might be the forked child process.

        try {
            File_.reset(new TFile(FileName_, CreateAlways | CloseOnExec));

            FileOutput_.reset(new TFileOutput(*File_));

            if (IsRaw_) {
                BufferedOutput_.reset(new TBufferedOutput(FileOutput_.get()));
                FacadeOutput_ = BufferedOutput_.get();
            } else {
                TSnapshotHeader header;
                WritePod(*File_, header);
                WritePadded(*File_, Meta_);
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
                        YUNREACHABLE();
                }
                LengthMeasureOutput_.reset(new TLengthMeasureOutputStream(CodecOutput_
                    ? CodecOutput_.get()
                    : ChecksumOutput_.get()));
                BufferedOutput_.reset(new TBufferedOutput(LengthMeasureOutput_.get()));
                FacadeOutput_ = BufferedOutput_.get();
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error opening snapshot %v for writing",
                FileName_)
                << ex;
        }
    }

    virtual TOutputStream* GetStream() override
    {
        return FacadeOutput_;
    }

    virtual void Close() override
    {
        // NB: Avoid logging here, this might be the forked child process.

        // NB: Some calls might be redundant.
        FacadeOutput_->Finish();
        BufferedOutput_->Finish();
        if (LengthMeasureOutput_) {
            LengthMeasureOutput_->Finish();
        }
        if (CodecOutput_) {
            CodecOutput_->Finish();
        }
        if (ChecksumOutput_) {
            ChecksumOutput_->Finish();
        }
        FileOutput_->Finish();

        if (!IsRaw_) {
            TSnapshotHeader header;
            header.SnapshotId = SnapshotId_;
            header.CompressedLength = File_->GetLength();
            header.UncompressedLength = LengthMeasureOutput_->GetLength();
            header.Checksum = ChecksumOutput_->GetChecksum();
            header.Codec = Codec_;
            header.MetaSize = Meta_.Size();
            File_->Seek(0, sSet);
            WritePod(*File_, header);
        }

        File_->Flush();
        File_->Close();
    }

private:
    Stroka FileName_;
    ECodec Codec_;
    int SnapshotId_;
    TSharedRef Meta_;
    bool IsRaw_;

    std::unique_ptr<TFile> File_;
    std::unique_ptr<TFileOutput> FileOutput_;
    std::unique_ptr<TOutputStream> CodecOutput_;
    std::unique_ptr<TChecksumOutput> ChecksumOutput_;
    std::unique_ptr<TLengthMeasureOutputStream> LengthMeasureOutput_;
    std::unique_ptr<TOutputStream> CheckpointableOutput_;
    std::unique_ptr<TOutputStream> BufferedOutput_;
    TOutputStream* FacadeOutput_;

};

ISnapshotWriterPtr CreateFileSnapshotWriter(
    const Stroka& fileName,
    ECodec codec,
    int snapshotId,
    const TSharedRef& meta,
    bool isRaw)
{
    auto writer = New<TFileSnapshotWriter>(
        fileName,
        codec,
        snapshotId,
        meta,
        isRaw);
    writer->Open();
    return writer;
}

////////////////////////////////////////////////////////////////////////////////

class TFileSnapshotStore::TImpl
    : public TRefCounted
{
public:
    explicit TImpl(TLocalSnapshotStoreConfigPtr config)
        : Config_(config)
        , Logger(HydraLogger)
    {
        Logger.AddTag("Path: %v", Config_->Path);
    }

    void Initialize()
    {
        auto path = Config_->Path;
        
        LOG_INFO("Preparing snapshot directory");
        
        NFS::ForcePath(path);
        NFS::CleanTempFiles(path);
        
        LOG_INFO("Looking for snapshots");
        
        auto fileNames = EnumerateFiles(path);
        for (const auto& fileName : fileNames) {
            auto extension = NFS::GetFileExtension(fileName);
            if (extension == SnapshotExtension) {
                auto name = NFS::GetFileNameWithoutExtension(fileName);
                try {
                    int snapshotId = FromString<int>(name);
                    RegisterSnapshot(snapshotId);
                } catch (const std::exception&) {
                    LOG_WARNING("Found unrecognized file %Qv", fileName);
                }
            }
        }
        
        LOG_INFO("Snapshot scan complete");
    }

    bool CheckSnapshotExists(int snapshotId)
    {
        auto path = GetSnapshotPath(snapshotId);
        if (NFS::Exists(path)) {
            return true;
        }

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (SnapshotIds_.erase(snapshotId) == 1) {
                LOG_WARNING("Erased orphaned snapshot %v from store", snapshotId);
            }
        }

        return false;
    }

    ISnapshotReaderPtr CreateReader(int snapshotId)
    {
        if (!CheckSnapshotExists(snapshotId)) {
            THROW_ERROR_EXCEPTION("No such snapshot %v", snapshotId);
        }

        return CreateFileSnapshotReader(
            GetSnapshotPath(snapshotId),
            snapshotId,
            false);
    }

    ISnapshotReaderPtr CreateRawReader(int snapshotId, i64 offset)
    {
        return CreateFileSnapshotReader(
            GetSnapshotPath(snapshotId),
            snapshotId,
            true,
            offset);
    }

    ISnapshotWriterPtr CreateWriter(int snapshotId, const TSharedRef& meta)
    {
        return CreateFileSnapshotWriter(
            GetSnapshotPath(snapshotId) + TempFileSuffix,
            Config_->Codec,
            snapshotId,
            meta,
            false);
    }

    ISnapshotWriterPtr CreateRawWriter(int snapshotId)
    {
        return CreateFileSnapshotWriter(
            GetSnapshotPath(snapshotId) + TempFileSuffix,
            Config_->Codec,
            snapshotId,
            TSharedRef(),
            true);
    }

    int GetLatestSnapshotId(int maxSnapshotId)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        auto it = SnapshotIds_.upper_bound(maxSnapshotId);
        if (it == SnapshotIds_.begin()) {
            return NonexistingSegmentId;
        }

        int snapshotId = *(--it);
        YCHECK(snapshotId <= maxSnapshotId);
        return snapshotId;
    }

    void ConfirmSnapshot(int snapshotId)
    {
        auto path = GetSnapshotPath(snapshotId);
        NFS::Rename(path + TempFileSuffix, path);
        RegisterSnapshot(snapshotId);
    }

private:
    TLocalSnapshotStoreConfigPtr Config_;

    NLog::TLogger Logger;

    TSpinLock SpinLock_;
    std::set<int> SnapshotIds_;



    Stroka GetSnapshotPath(int snapshotId)
    {
        return NFS::CombinePaths(
            Config_->Path,
            Format("%09d.%v", snapshotId, SnapshotExtension));
    }

    void RegisterSnapshot(int snapshotId)
    {
        TGuard<TSpinLock> guard(SpinLock_);
        YCHECK(SnapshotIds_.insert(snapshotId).second);
        LOG_INFO("Registered snapshot %v", snapshotId);
    }

};

////////////////////////////////////////////////////////////////////////////////

TFileSnapshotStore::TFileSnapshotStore(TLocalSnapshotStoreConfigPtr config)
    : Impl_(New<TImpl>(config))
{ }

TFileSnapshotStore::~TFileSnapshotStore()
{ }

void TFileSnapshotStore::Initialize()
{
    Impl_->Initialize();
}

bool TFileSnapshotStore::CheckSnapshotExists(int snapshotId)
{
    return Impl_->CheckSnapshotExists(snapshotId);
}

ISnapshotReaderPtr TFileSnapshotStore::CreateReader(int snapshotId)
{
    return Impl_->CreateReader(snapshotId);
}

ISnapshotReaderPtr TFileSnapshotStore::CreateRawReader(int snapshotId, i64 offset)
{
    return Impl_->CreateRawReader(snapshotId, offset);
}

ISnapshotWriterPtr TFileSnapshotStore::CreateWriter(int snapshotId, const TSharedRef& meta)
{
    return Impl_->CreateWriter(snapshotId, meta);
}

ISnapshotWriterPtr TFileSnapshotStore::CreateRawWriter(int snapshotId)
{
    return Impl_->CreateRawWriter(snapshotId);
}

int TFileSnapshotStore::GetLatestSnapshotId(int maxSnapshotId)
{
    return Impl_->GetLatestSnapshotId(maxSnapshotId);
}

void TFileSnapshotStore::ConfirmSnapshot(int snapshotId)
{
    Impl_->ConfirmSnapshot(snapshotId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
