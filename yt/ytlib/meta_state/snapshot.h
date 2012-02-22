#pragma once

#include "common.h"

#include <ytlib/misc/checksum.h>

#include <util/stream/file.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotReader
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TSnapshotReader> TPtr;

    TSnapshotReader(const Stroka& fileName, i32 segmentId);

    void Open();
    TInputStream& GetStream() const;
    i64 GetLength() const;
    TChecksum GetChecksum() const;
    i32 GetPrevRecordCount() const;

private:
    Stroka FileName;
    i32 SegmentId;
    TChecksum Checksum;
    i32 PrevRecordCount;

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
    typedef TIntrusivePtr<TSnapshotWriter> TPtr;

    TSnapshotWriter(
        Stroka fileName,
        i32 segmentId);

    void Open(i32 prevRecordCount);
    
    TOutputStream& GetStream() const;
    void Close();
    TChecksum GetChecksum() const;

private:
    Stroka FileName;
    Stroka TempFileName;
    i32 SegmentId;
    i32 PrevRecordCount;
    TChecksum Checksum;

    THolder<TFile> File;
    THolder<TBufferedFileOutput> FileOutput;
    THolder<TOutputStream> CompressedOutput;
    THolder<TChecksummableOutput> ChecksummableOutput;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
