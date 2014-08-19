#include "stdafx.h"
#include "file_helpers.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TBufferedFile::TBufferedFile(
    const Stroka& fileName,
    ui32 oMode,
    size_t bufferSize)
    : File_(fileName, oMode)
    , FileOutput_(File_, bufferSize)
{ }

i64 TBufferedFile::Seek(i64 offset, SeekDir origin)
{
    FileOutput_.Flush();
    return File_.Seek(offset, origin);
}

void TBufferedFile::Flush()
{
    FileOutput_.Flush();
    File_.Flush();
}

void TBufferedFile::Append(const void* buffer, size_t length)
{
    FileOutput_.Write(buffer, length);
}

void TBufferedFile::Write(const void* buffer, size_t length)
{
    FileOutput_.Flush();
    File_.Write(buffer, length);
}

size_t TBufferedFile::Pread(void* buffer, size_t length, i64 offset)
{
    FileOutput_.Flush();
    return File_.Pread(buffer, length, offset);
}

size_t TBufferedFile::Load(void* buffer, size_t length)
{
    FileOutput_.Flush();
    File_.Read(buffer, length);
    return length;
}

void TBufferedFile::Skip(size_t length)
{
    FileOutput_.Flush();
    File_.Seek(length, sCur);
}

size_t TBufferedFile::GetPosition()
{
    FileOutput_.Flush();
    return File_.GetPosition();
}

size_t TBufferedFile::GetLength()
{
    FileOutput_.Flush();
    return File_.GetLength();
}

void TBufferedFile::Resize(size_t length)
{
    FileOutput_.Flush();
    File_.Resize(length);
}

void TBufferedFile::Close()
{
    FileOutput_.Flush();
    File_.Close();
}

void TBufferedFile::Flock(int op)
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

} // namespace NHydra
} // namespace NYT
