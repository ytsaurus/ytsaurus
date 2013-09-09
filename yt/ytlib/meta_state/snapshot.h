#pragma once

#include "public.h"
#include "file_helpers.h"

#include <core/misc/checksum.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotHeader;

////////////////////////////////////////////////////////////////////////////////

class TSnapshotReader
    : public TRefCounted
{
public:
    TSnapshotReader(
        const Stroka& fileName,
        i32 snapshotId,
        bool enableCompression);
    ~TSnapshotReader();

    void Open();

    TInputStream* GetStream() const;
    i64 GetLength() const;
    TChecksum GetChecksum() const;
    const TEpochId& GetEpoch() const;
    i32 GetPrevRecordCount() const;

private:
    Stroka FileName;
    i32 SnapshotId;
    bool EnableCompression;

    std::unique_ptr<TSnapshotHeader> Header;

    std::unique_ptr<TFile> File;
    std::unique_ptr<TBufferedFileInput> FileInput;
    std::unique_ptr<TInputStream> DecompressedInput;
    std::unique_ptr<TChecksumInput> ChecksummableInput;
};

////////////////////////////////////////////////////////////////////////////////

class TSnapshotWriter
    : public TRefCounted
{
public:
    TSnapshotWriter(
        const Stroka& fileName,
        i32 snapshotId,
        bool enableCompression);
    ~TSnapshotWriter();

    void Open(i32 prevRecordCount, const TEpochId& epoch);

    TOutputStream* GetStream() const;
    void Close();
    TChecksum GetChecksum() const;

private:
    DECLARE_ENUM(EState,
        (Uninitialized)
        (Opened)
        (Closed)
    );
    EState State;

    Stroka FileName;
    Stroka TempFileName;
    bool EnableCompression;

    std::unique_ptr<TSnapshotHeader> Header;

    std::unique_ptr<TBufferedFile> File;
    std::unique_ptr<TOutputStream> CompressedOutput;
    std::unique_ptr<TBufferedOutput> BufferedOutput;
    std::unique_ptr<TChecksumOutput> ChecksummableOutput;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
