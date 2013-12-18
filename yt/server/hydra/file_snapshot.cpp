#include "stdafx.h"
#include "file_snapshot.h"
#include "snapshot.h"
#include "config.h"
#include "private.h"
#include "file_helpers.h"

#include <core/misc/fs.h>
#include <core/misc/serialize.h>
#include <core/misc/checksum.h>

#include <core/concurrency/thread_affinity.h>

#include <core/logging/tagged_logger.h>

#include <util/stream/lz.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NHydra {

using namespace NFS;
using namespace NCompression;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = HydraLogger;

static const char* const SnapshotExtension = "snapshot";

////////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

struct TSnapshotHeader
{
    static const ui64 ExpectedSignature =  0x3230303053535459ull; // YTSS0002

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

static_assert(sizeof(TSnapshotHeader) == 44, "Binary size of TSnapshotHeader has changed.");

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

class TFileSnapshotStore;
typedef TIntrusivePtr<TFileSnapshotStore> TFileSnapshotStorePtr;

class TFileSnapshotStore
    : public ISnapshotStore
{
public:
    explicit TFileSnapshotStore(
        const TCellGuid& cellGuid,
        TFileSnapshotStoreConfigPtr config)
        : CellGuid(cellGuid)
        , Config(config)
        , Logger(HydraLogger)
    {
        Logger.AddTag(Sprintf("Path: %s", ~Config->Path));
    }

    void Initialize()
    {
        auto path = Config->Path;
        
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
                    OnSnapshotAdded(snapshotId);
                } catch (const std::exception&) {
                    LOG_WARNING("Found unrecognized file %s", ~fileName.Quote());
                }
            }
        }
        
        LOG_INFO("Snapshot scan complete");
    }

    virtual const TCellGuid& GetCellGuid() const
    {
        return CellGuid;
    }

    virtual TNullable<TSnapshotParams> TryGetSnapshotParams(int snapshotId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        if (!CheckSnapshotExists(snapshotId)) {
            return Null;
        }

        TGuard<TSpinLock> guard(SpinLock);
        auto it = SnapshotMap.find(snapshotId);
        if (it == SnapshotMap.end()) {
            return Null;
        }

        return it->second;
    }

    virtual ISnapshotReaderPtr TryCreateReader(int snapshotId) override
    {
        if (!CheckSnapshotExists(snapshotId)) {
            return nullptr;
        }

        auto reader = New<TReader>(this, snapshotId, false);
        reader->Open();
        return reader;
    }

    virtual ISnapshotReaderPtr TryCreateRawReader(
        int snapshotId,
        i64 offset) override
    {
        auto reader = New<TReader>(this, snapshotId, true);
        reader->Open(offset);
        return reader;
    }

    virtual ISnapshotWriterPtr CreateWriter(
        int snapshotId,
        const TSnapshotCreateParams& params) override
    {
        auto writer = New<TWriter>(
            this,
            snapshotId,
            params,
            false);
        writer->Open();
        return writer;
    }

    virtual ISnapshotWriterPtr CreateRawWriter(int snapshotId) override
    {
        auto writer = New<TWriter>(
            this,
            snapshotId,
            TSnapshotCreateParams(),
            true);
        writer->Open();
        return writer;
    }

    virtual int GetLatestSnapshotId(int maxSnapshotId) override
    {
        VERIFY_THREAD_AFFINITY_ANY();
        
        TGuard<TSpinLock> guard(SpinLock);

        auto it = SnapshotMap.upper_bound(maxSnapshotId);
        if (it == SnapshotMap.begin()) {
            return NonexistingSegmentId;
        }

        int snapshotId = (--it)->first;
        YCHECK(snapshotId <= maxSnapshotId);
        return snapshotId;
    }

private:
    TCellGuid CellGuid;
    TFileSnapshotStoreConfigPtr Config;

    NLog::TTaggedLogger Logger;

    TSpinLock SpinLock;
    std::map<int, TSnapshotParams> SnapshotMap;


    class TReader
        : public ISnapshotReader
    {
    public:
        TReader(
            TFileSnapshotStorePtr store,
            int snapshotId,
            bool isRaw)
            : Store(store)
            , SnapshotId(snapshotId)
            , IsRaw(isRaw)
            , Logger(Store->Logger)
            , FacadeInput(nullptr)
        { }

        void Open(i64 offset = -1)
        {
            auto path = Store->GetSnapshotPath(SnapshotId);
            try {
                File.reset(new TFile(path, OpenExisting | CloseOnExec));

                TSnapshotHeader header;
                ReadPod(*File, header);
                header.Validate();

                if (header.SnapshotId != SnapshotId) {
                    THROW_ERROR_EXCEPTION(
                        "Invalid snapshot id in header of %s: expected %d, got %d",
                        ~path.Quote(),
                        SnapshotId,
                        header.SnapshotId);
                }

                if (header.CompressedLength != File->GetLength()) {
                    THROW_ERROR_EXCEPTION(
                        "Invalid compressed length in header of %s: expected %" PRId64 ", got %" PRId64,
                        ~path.Quote(),
                        File->GetLength(),
                        header.CompressedLength);
                }

                if (IsRaw) {
                    File->Seek(offset, sSet);
                }

                RawInput.reset(new TBufferedFileInput(*File));

                auto codec = ECodec(header.Codec);
                if (IsRaw || codec == ECodec::None) {
                    FacadeInput = RawInput.get();
                } else {
                    switch (codec) {
                        case ECodec::Snappy:
                            CodecInput.reset(new TSnappyDecompress(RawInput.get()));
                            break;
                        case ECodec::Lz4:
                            CodecInput.reset(new TLz4Decompress(RawInput.get()));
                            break;
                        default:
                            YUNREACHABLE();
                    }
                    FacadeInput = CodecInput.get();
                }

            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error opening snapshot %s for reading",
                    ~path.Quote())
                    << ex;
            }
        }

        virtual TInputStream* GetStream() const override
        {
            return FacadeInput;
        }

    private:
        TFileSnapshotStorePtr Store;
        int SnapshotId;
        bool IsRaw;

        NLog::TTaggedLogger& Logger;

        std::unique_ptr<TFile> File;
        std::unique_ptr<TBufferedFileInput> RawInput;
        std::unique_ptr<TInputStream> CodecInput;
        TInputStream* FacadeInput;

    };

    class TWriter
        : public ISnapshotWriter
    {
    public:
        TWriter(
            TFileSnapshotStorePtr store,
            int snapshotId,
            const TSnapshotCreateParams& params,
            bool isRaw)
            : Store(store)
            , SnapshotId(snapshotId)
            , Params(params)
            , IsRaw(isRaw)
            , IsClosed(false)
            , Logger(HydraLogger)
            , FacadeOutput(nullptr)
        {
            Path = Store->GetSnapshotPath(SnapshotId);
            Logger.AddTag(Sprintf("Path: %s",
                ~Path));
        }

        void Open()
        {
            auto path = Path + TempFileSuffix;
            try {
                File.reset(new TFile(path, CreateAlways | CloseOnExec));

                RawOutput.reset(new TBufferedFileOutput(*File));

                if (IsRaw) {
                    FacadeOutput = RawOutput.get();
                } else {
                    TSnapshotHeader header;
                    WritePod(*File, header);
                    File->Flush();

                    ChecksumOutput.reset(new TChecksumOutput(RawOutput.get()));

                    auto codec = Store->Config->Codec;
                    if (codec == ECodec::None) {
                        LengthMeasureOutput.reset(new TLengthMeasureOutputStream(ChecksumOutput.get()));
                    } else {
                        switch (codec) {
                            case ECodec::Snappy:
                                CodecOutput.reset(new TSnappyCompress(ChecksumOutput.get()));
                                break;
                            case ECodec::Lz4:
                                CodecOutput.reset(new TLz4Compress(ChecksumOutput.get()));
                                break;
                            default:
                                YUNREACHABLE();
                        }
                        LengthMeasureOutput.reset(new TLengthMeasureOutputStream(CodecOutput.get()));
                    }

                    FacadeOutput = LengthMeasureOutput.get();
                }
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Error opening snapshot %s for writing",
                    ~path.Quote())
                    << ex;
            }

            LOG_INFO("Snapshot writer opened");
        }

        virtual TOutputStream* GetStream() const override
        {
            return FacadeOutput;
        }

        virtual void Close() override
        {
            // NB: Avoid logging here, this might be the forked child process.

            DoFinish();

            if (!IsRaw) {
                TSnapshotHeader header;
                header.SnapshotId = SnapshotId;
                header.PrevRecordCount = Params.PrevRecordCount;
                header.CompressedLength = File->GetLength();
                header.UncompressedLength = LengthMeasureOutput->GetLength();
                header.Checksum = ChecksumOutput->GetChecksum();
                header.Codec = Store->Config->Codec;
                File->Seek(0, sSet);
                WritePod(*File, header);
            }

            DoClose();
        }

        virtual void Confirm() override
        {
            DoClose();

            auto path = Store->GetSnapshotPath(SnapshotId);
            if (!Rename(path + TempFileSuffix, path)) {
                LOG_FATAL("Error renaming snapshot %s",
                    ~path.Quote());
            }

            LOG_INFO("Snapshot writer closed and confirmed");

            Store->OnSnapshotAdded(SnapshotId);
        }

    private:
        TFileSnapshotStorePtr Store;
        int SnapshotId;
        TSnapshotCreateParams Params;
        bool IsRaw;

        Stroka Path;
        bool IsClosed;

        NLog::TTaggedLogger Logger;

        std::unique_ptr<TFile> File;
        std::unique_ptr<TBufferedFileOutput> RawOutput;
        std::unique_ptr<TOutputStream> CodecOutput;
        std::unique_ptr<TChecksumOutput> ChecksumOutput;
        std::unique_ptr<TLengthMeasureOutputStream> LengthMeasureOutput;
        TOutputStream* FacadeOutput;

        void DoFinish()
        {
            // NB: Some calls might be redundant.
            FacadeOutput->Finish();
            if (LengthMeasureOutput) {
                LengthMeasureOutput->Finish();
            }
            if (CodecOutput) {
                CodecOutput->Finish();
            }
            if (ChecksumOutput) {
                ChecksumOutput->Finish();
            }
            RawOutput->Finish();
            File->Flush();
        }

        void DoClose()
        {
            if (IsClosed)
                return;

            File->Close();

            IsClosed = true;
        }

    };


    Stroka GetSnapshotPath(int snapshotId)
    {
        return NFS::CombinePaths(Config->Path, Sprintf("%09d.%s", snapshotId, SnapshotExtension));
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
        if (isexist(~path)) {
            return true;
        }

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (SnapshotMap.erase(snapshotId) == 1) {
                LOG_WARNING("Erased orphaned snapshot id snapshot %d from store",
                    snapshotId);
            }
        }

        return false;
    }

    void OnSnapshotAdded(int snapshotId)
    {
        VERIFY_THREAD_AFFINITY_ANY();
    
        auto params = ReadSnapshotParams(snapshotId);

        {
            TGuard<TSpinLock> guard(SpinLock);
            YCHECK(SnapshotMap.insert(std::make_pair(snapshotId, params)).second);
        }

        LOG_INFO("Registered snapshot %d", snapshotId);
    }

};

ISnapshotStorePtr CreateFileSnapshotStore(
    const TCellGuid& cellGuid,
    TFileSnapshotStoreConfigPtr config)
{
    auto store = New<TFileSnapshotStore>(
        cellGuid,
        config);
    store->Initialize();
    return store;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
