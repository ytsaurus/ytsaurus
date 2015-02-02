#include "stdafx.h"
#include "file_helpers.h"
#include "private.h"

#include <core/misc/fs.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TFileWrapper::TFileWrapper(const Stroka& fileName, ui32 oMode)
    : File_(fileName, oMode)
{ }

i64 TFileWrapper::Seek(i64 offset, SeekDir origin)
{
    return File_.Seek(offset, origin);
}

void TFileWrapper::Flush()
{
    File_.Flush();
}

void TFileWrapper::Write(const void* buffer, size_t length)
{
    File_.Write(buffer, length);
}

size_t TFileWrapper::Pread(void* buffer, size_t length, i64 offset)
{
    return File_.Pread(buffer, length, offset);
}

size_t TFileWrapper::Load(void* buffer, size_t length)
{
    File_.Read(buffer, length);
    return length;
}

void TFileWrapper::Skip(size_t length)
{
    File_.Seek(length, sCur);
}

size_t TFileWrapper::GetPosition()
{
    return File_.GetPosition();
}

size_t TFileWrapper::GetLength()
{
    return File_.GetLength();
}

void TFileWrapper::Resize(size_t length)
{
    File_.Resize(length);
}

void TFileWrapper::Close()
{
    File_.Close();
}

void TFileWrapper::Flock(int op)
{
    File_.Flock(op);
}

////////////////////////////////////////////////////////////////////////////////

TLengthMeasureOutputStream::TLengthMeasureOutputStream(TOutputStream* output)
    : Output(output)
    , Length(0)
{ }

i64 TLengthMeasureOutputStream::GetLength() const
{
    return Length;
}

void TLengthMeasureOutputStream::DoWrite(const void* buf, size_t len)
{
    Output->Write(buf, len);
    Length += len;
}

void TLengthMeasureOutputStream::DoFlush()
{
    Output->Flush();
}

void TLengthMeasureOutputStream::DoFinish()
{
    Output->Finish();
}

////////////////////////////////////////////////////////////////////////////////

void RemoveChangelogFiles(const Stroka& path)
{
    auto dataFileName = path;
    NFS::Remove(dataFileName);

    auto indexFileName = path + ChangelogIndexSuffix;
    if (NFS::Exists(indexFileName)) {
        NFS::Remove(indexFileName);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
