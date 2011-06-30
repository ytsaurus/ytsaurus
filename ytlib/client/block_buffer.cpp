#include "block_buffer.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TBlockBuffer::TBlockBuffer()
    : SizeLimit(0)
{}

TBlockBuffer::~TBlockBuffer()
{}

void TBlockBuffer::SetSizeLimit(ui32 sizeLimit)
{
    SizeLimit = sizeLimit;
}

void TBlockBuffer::Clear()
{
    Buffer.yresize(SizeLimit);
    Excess.yresize(0);
    WritePos = 0;
    ReadPos = 0;
}

size_t TBlockBuffer::Sizeof(size_t number) const
{
    // TODO: varints
    (void)number;
    return sizeof(size_t);
}

char* TBlockBuffer::GetPtr(ui32 pos) const
{
    if (pos < Buffer.size())
        return (char*)(Buffer.begin() + pos);
    else
        return (char*)(Excess.begin() + pos - Buffer.size());
}

void TBlockBuffer::Allocate(size_t size)
{
    if (WritePos >= Buffer.size())
        Excess.yresize(WritePos + size - Buffer.size());

    else if (WritePos + size > Buffer.size()) {
        Buffer.yresize(WritePos);
        Excess.yresize(size);
    }
}

void TBlockBuffer::Append(size_t number)
{
    // TODO: varints
    *(size_t*)GetPtr(WritePos) = number;
    WritePos += sizeof(size_t);
}

void TBlockBuffer::Append(const char* data, size_t size)
{
    memcpy(GetPtr(WritePos), data, size);
    WritePos += size;
}

void TBlockBuffer::SetData(TBlob* data)
{
    Buffer.swap(*data);
    Excess.yresize(0);
    WritePos = Buffer.size();
    ReadPos = 0;
}

ui32 TBlockBuffer::GetReadPos() const
{
    return ReadPos;
}

void TBlockBuffer::SetReadPos(ui32 pos)
{
    YASSERT(pos < WritePos);
    ReadPos = pos;
}

size_t TBlockBuffer::ReadNumber()
{
    // TODO: varints
    char* ptr = GetPtr(ReadPos);
    ReadPos += sizeof(size_t);
    return *(size_t*)ptr;
}

TValue TBlockBuffer::ReadValue()
{
    size_t len = ReadNumber();
    if (!len)
        return TValue(); // not set

    --len;
    char* ptr = GetPtr(ReadPos);
    ReadPos += len;
    return TValue(ptr, len);
}

void TBlockBuffer::GetData(TBlob* data)
{
    Buffer.yresize(WritePos);
    Excess.yresize(0);
    ReadPos = 0;
    Buffer.swap(*data);
}

ui32 TBlockBuffer::GetWritePos() const
{
    return WritePos;
}

size_t TBlockBuffer::WriteNumber(size_t number)
{
    Allocate(Sizeof(number));

    ui32 pos = WritePos;
    Append(number);
    return WritePos - pos;
}

size_t TBlockBuffer::WriteValue(const TValue& value)
{
    size_t len = value.GetSize();
    Allocate(Sizeof(len + 1) + len);

    ui32 pos = WritePos;
    Append(len + 1);
    Append(value.GetData(), len);
    return WritePos - pos;
}

////////////////////////////////////////////////////////////////////////////////

}
