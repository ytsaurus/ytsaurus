#pragma once

#include "public.h"
#include "file_helpers.h"

#include <ytlib/misc/checksum.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotHeader;

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

    THolder<TSnapshotHeader> Header;

    THolder<TFile> File;
    THolder<TBufferedFileInput> FileInput;
    THolder<TInputStream> DecompressedInput;
    THolder<TChecksummableInput> ChecksummableInput;
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

    THolder<TSnapshotHeader> Header;

    THolder<TBufferedFile> File;
    THolder<TOutputStream> CompressedOutput;
    THolder<TBufferedOutput> BufferedOutput;
    THolder<TChecksummableOutput> ChecksummableOutput;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
