#include "stdafx.h"
#include "file_snapshot_store.h"
#include "snapshot.h"
#include "config.h"
#include "private.h"
#include "file_helpers.h"

#include <core/misc/fs.h>
#include <core/misc/serialize.h>
#include <core/misc/checksum.h>

#include <core/logging/tagged_logger.h>

#include <util/stream/lz.h>

namespace NYT {
namespace NHydra {

using namespace NFS;
using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HydraLogger;

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TSnapshotHeader
{
    static const ui64 ExpectedSignature;

    ui64 Signature;
    i32 SnapshotId;
    i32 PrevRecordCount;
    ui64 CompressedLength;
    ui64 UncompressedLength;
    ui64 Checksum;
    i32 Codec;

    TSnapshotHeader()
        : Signature(ExpectedSignature)
        , SnapshotId(0)
        , PrevRecordCount(0)
        , CompressedLength(0)
        , UncompressedLength(0)
        , Checksum(0)
        , Codec(ECodec::None)
    { }

    void Validate() const
    {
        if (Signature != ExpectedSignature) {
            LOG_FATAL("Invalid signature: expected %" PRIx64 ", found %" PRIx64,
                ExpectedSignature,
                Signature);
        }
    }
};

const ui64 TSnapshotHeader::ExpectedSignature =  0x3230303053535459ull; // YTSS0002

static_assert(sizeof(TSnapshotHeader) == 44, "Binary size of TSnapshotHeader has changed.");

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT

DECLARE_PODTYPE(NYT::NHydra::TSnapshotHeader)

namespace NYT {
namespace NHydra {

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
        , FacadeInput_(nullptr)
    {
        Logger.AddTag("FileName: %v", FileName_);
    }

    void Open(i64 offset)
    {
        try {
            File_.reset(new TFile(FileName_, OpenExisting | CloseOnExec));

            ReadPod(*File_, Header_);
            Header_.Validate();

            if (Header_.SnapshotId != SnapshotId_) {
                THROW_ERROR_EXCEPTION(
                    "Invalid snapshot id in header of %s: expected %d, got %d",
                    ~FileName_.Quote(),
                    SnapshotId_,
                    Header_.SnapshotId);
            }

            if (Header_.CompressedLength != File_->GetLength()) {
                THROW_ERROR_EXCEPTION(
                    "Invalid compressed length in header of %s: expected %" PRId64 ", got %" PRId64,
                    ~FileName_.Quote(),
                    File_->GetLength(),
                    Header_.CompressedLength);
            }

            if (IsRaw_) {
                File_->Seek(offset, sSet);
            }

            RawInput_.reset(new TBufferedFileInput(*File_));

            auto codec = ECodec(Header_.Codec);
            if (IsRaw_ || codec == ECodec::None) {
                FacadeInput_ = RawInput_.get();
            } else {
                switch (codec) {
                    case ECodec::Snappy:
                        CodecInput_.reset(new TSnappyDecompress(RawInput_.get()));
                        break;
                    case ECodec::Lz4:
                        CodecInput_.reset(new TLz4Decompress(RawInput_.get()));
                        break;
                    default:
                        YUNREACHABLE();
                }
                FacadeInput_ = CodecInput_.get();
            }

        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error opening snapshot %s for reading",
                ~FileName_.Quote())
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
        params.PrevRecordCount = Header_.PrevRecordCount;
        params.Checksum = Header_.Checksum;
        params.CompressedLength = Header_.CompressedLength;
        params.UncompressedLength = Header_.UncompressedLength;
        return params;
    }

private:
    Stroka FileName_;
    int SnapshotId_;
    bool IsRaw_;

    NLog::TTaggedLogger Logger;

    std::unique_ptr<TFile> File_;
    std::unique_ptr<TBufferedFileInput> RawInput_;
    std::unique_ptr<TInputStream> CodecInput_;
    TInputStream* FacadeInput_;

    TSnapshotHeader Header_;

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
        const TSnapshotCreateParams& params,
        bool isRaw)
        : FileName_(fileName)
        , Codec_(codec)
        , SnapshotId_(snapshotId)
        , Params_(params)
        , IsRaw_(isRaw)
        , FacadeOutput_(nullptr)
    { }

    void Open()
    {
        // NB: Avoid logging here, this might be the forked child process.

        try {
            File_.reset(new TFile(FileName_, CreateAlways | CloseOnExec));

            RawOutput_.reset(new TBufferedFileOutput(*File_));

            if (IsRaw_) {
                FacadeOutput_ = RawOutput_.get();
            } else {
                TSnapshotHeader header;
                WritePod(*File_, header);
                File_->Flush();

                ChecksumOutput_.reset(new TChecksumOutput(RawOutput_.get()));

                if (Codec_ == ECodec::None) {
                    LengthMeasureOutput_.reset(new TLengthMeasureOutputStream(ChecksumOutput_.get()));
                } else {
                    switch (Codec_) {
                        case ECodec::Snappy:
                            CodecOutput_.reset(new TSnappyCompress(ChecksumOutput_.get()));
                            break;
                        case ECodec::Lz4:
                            CodecOutput_.reset(new TLz4Compress(ChecksumOutput_.get()));
                            break;
                        default:
                            YUNREACHABLE();
                    }
                    LengthMeasureOutput_.reset(new TLengthMeasureOutputStream(CodecOutput_.get()));
                }

                FacadeOutput_ = LengthMeasureOutput_.get();
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error opening snapshot %s for writing",
                ~FileName_.Quote())
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
        if (LengthMeasureOutput_) {
            LengthMeasureOutput_->Finish();
        }
        if (CodecOutput_) {
            CodecOutput_->Finish();
        }
        if (ChecksumOutput_) {
            ChecksumOutput_->Finish();
        }
        RawOutput_->Finish();

        if (!IsRaw_) {
            TSnapshotHeader header;
            header.SnapshotId = SnapshotId_;
            header.PrevRecordCount = Params_.PrevRecordCount;
            header.CompressedLength = File_->GetLength();
            header.UncompressedLength = LengthMeasureOutput_->GetLength();
            header.Checksum = ChecksumOutput_->GetChecksum();
            header.Codec = Codec_;
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
    TSnapshotCreateParams Params_;
    bool IsRaw_;

    std::unique_ptr<TFile> File_;
    std::unique_ptr<TBufferedFileOutput> RawOutput_;
    std::unique_ptr<TOutputStream> CodecOutput_;
    std::unique_ptr<TChecksumOutput> ChecksumOutput_;
    std::unique_ptr<TLengthMeasureOutputStream> LengthMeasureOutput_;
    TOutputStream* FacadeOutput_;

};

ISnapshotWriterPtr CreateFileSnapshotWriter(
    const Stroka& fileName,
    ECodec codec,
    int snapshotId,
    const TSnapshotCreateParams& params,
    bool isRaw)
{
    auto writer = New<TFileSnapshotWriter>(
        fileName,
        codec,
        snapshotId,
        params,
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
                    LOG_WARNING("Found unrecognized file %s", ~fileName.Quote());
                }
            }
        }
        
        LOG_INFO("Snapshot scan complete");
    }

    TNullable<TSnapshotParams> FindSnapshotParams(int snapshotId)
    {
        if (!CheckSnapshotExists(snapshotId)) {
            return Null;
        }

        TGuard<TSpinLock> guard(SpinLock_);
        auto it = SnapshotMap_.find(snapshotId);
        if (it == SnapshotMap_.end()) {
            return Null;
        }

        return it->second;
    }

    ISnapshotReaderPtr CreateReader(int snapshotId)
    {
        if (!CheckSnapshotExists(snapshotId)) {
            THROW_ERROR_EXCEPTION("No such snapshot %d", snapshotId);
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

    ISnapshotWriterPtr CreateWriter(int snapshotId, const TSnapshotCreateParams& params)
    {
        return CreateFileSnapshotWriter(
            GetSnapshotPath(snapshotId) + TempFileSuffix,
            Config_->Codec,
            snapshotId,
            params,
            false);
    }

    ISnapshotWriterPtr CreateRawWriter(int snapshotId)
    {
        return CreateFileSnapshotWriter(
            GetSnapshotPath(snapshotId) + TempFileSuffix,
            Config_->Codec,
            snapshotId,
            TSnapshotCreateParams(),
            true);
    }

    int GetLatestSnapshotId(int maxSnapshotId)
    {
        TGuard<TSpinLock> guard(SpinLock_);

        auto it = SnapshotMap_.upper_bound(maxSnapshotId);
        if (it == SnapshotMap_.begin()) {
            return NonexistingSegmentId;
        }

        int snapshotId = (--it)->first;
        YCHECK(snapshotId <= maxSnapshotId);
        return snapshotId;
    }

    TSnapshotParams ConfirmSnapshot(int snapshotId)
    {
        auto path = GetSnapshotPath(snapshotId);
        NFS::Rename(path + TempFileSuffix, path);
        return RegisterSnapshot(snapshotId);
    }

private:
    TLocalSnapshotStoreConfigPtr Config_;

    NLog::TTaggedLogger Logger;

    TSpinLock SpinLock_;
    std::map<int, TSnapshotParams> SnapshotMap_;



    Stroka GetSnapshotPath(int snapshotId)
    {
        return NFS::CombinePaths(
            Config_->Path,
            Format("%09d.%v", snapshotId, SnapshotExtension));
    }

    TSnapshotParams ReadSnapshotParams(int snapshotId)
    {
        auto fileName = GetSnapshotPath(snapshotId);
        TSnapshotParams params;
        try {
            TFile file(fileName, OpenExisting|CloseOnExec);
            TFileInput input(file);
            TSnapshotHeader header;
            ReadPod(input, header);
            header.Validate();
            params.PrevRecordCount = header.PrevRecordCount;
            params.Checksum = header.Checksum;
            params.CompressedLength = header.CompressedLength;
            params.UncompressedLength = header.UncompressedLength;
        } catch (const std::exception& ex) {
            LOG_FATAL(ex, "Error reading header of snapshot %s",
                ~fileName.Quote());
        }
        return params;
    }

    bool CheckSnapshotExists(int snapshotId)
    {
        auto path = GetSnapshotPath(snapshotId);
        if (NFS::Exists(path)) {
            return true;
        }

        {
            TGuard<TSpinLock> guard(SpinLock_);
            if (SnapshotMap_.erase(snapshotId) == 1) {
                LOG_WARNING("Erased orphaned snapshot %d from store", snapshotId);
            }
        }

        return false;
    }

    TSnapshotParams RegisterSnapshot(int snapshotId)
    {
        auto params = ReadSnapshotParams(snapshotId);

        {
            TGuard<TSpinLock> guard(SpinLock_);
            YCHECK(SnapshotMap_.insert(std::make_pair(snapshotId, params)).second);
        }

        LOG_INFO("Registered snapshot %d", snapshotId);
        return params;
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

ISnapshotReaderPtr TFileSnapshotStore::CreateReader(int snapshotId)
{
    return Impl_->CreateReader(snapshotId);
}

ISnapshotReaderPtr TFileSnapshotStore::CreateRawReader(int snapshotId, i64 offset)
{
    return Impl_->CreateRawReader(snapshotId, offset);
}

ISnapshotWriterPtr TFileSnapshotStore::CreateWriter(int snapshotId, const TSnapshotCreateParams& params)
{
    return Impl_->CreateWriter(snapshotId, params);
}

ISnapshotWriterPtr TFileSnapshotStore::CreateRawWriter(int snapshotId)
{
    return Impl_->CreateRawWriter(snapshotId);
}

int TFileSnapshotStore::GetLatestSnapshotId(int maxSnapshotId)
{
    return Impl_->GetLatestSnapshotId(maxSnapshotId);
}

TSnapshotParams TFileSnapshotStore::ConfirmSnapshot(int snapshotId)
{
    return Impl_->ConfirmSnapshot(snapshotId);
}

TNullable<TSnapshotParams> TFileSnapshotStore::FindSnapshotParams(int snapshotId)
{
    return Impl_->FindSnapshotParams(snapshotId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
