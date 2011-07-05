#pragma once

#include "common.h"

#include "../misc/checksum.h"

#include <util/stream/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotReader
    : private TNonCopyable
    , public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSnapshotReader> TPtr;

    TSnapshotReader(Stroka fileName, i32 segmentId);

    void Open(i64 offset = 0);
    TInputStream& GetStream() const;
    i64 GetLength() const;
    void Close();
    TChecksum GetChecksum() const;
    i32 GetPrevRecordCount() const;

private:
    Stroka FileName;
    i32 SegmentId;
    TChecksum Checksum;
    i32 PrevRecordCount;

    THolder<TFile> File;
    THolder<TBufferedFileInput> FileInput;
    THolder<TChecksummableInput> ChecksummableInput;
};

////////////////////////////////////////////////////////////////////////////////

class TSnapshotWriter
    : private TNonCopyable
    , public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSnapshotWriter> TPtr;

    TSnapshotWriter(Stroka fileName, i32 segmentId);

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
    THolder<TChecksummableOutput> ChecksummableOutput;
};

////////////////////////////////////////////////////////////////////////////////

}
