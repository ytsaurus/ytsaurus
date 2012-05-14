#include "stdafx.h"
#include "blob_output.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TBlobOutput::TBlobOutput()
{ }

TBlobOutput::TBlobOutput(size_t capacity)
{
    Blob.reserve(capacity);
}

TBlobOutput::~TBlobOutput() throw()
{ }

void TBlobOutput::DoWrite(const void* buf, size_t len)
{
    Blob.insert(
        Blob.end(),
        static_cast<const char*>(buf),
        static_cast<const char*>(buf) + len);
}

const char* TBlobOutput::Begin() const
{
    return Blob.begin();
}

i32 TBlobOutput::GetSize() const
{
    return static_cast<i32>(Blob.size());
}

void TBlobOutput::Clear()
{
    Blob.clear();
}

TSharedRef TBlobOutput::Flush(size_t size)
{
    auto result = TSharedRef(MoveRV(Blob));
    Blob.reserve(size);
    return result;
}

const TBlob* TBlobOutput::GetBlob() const
{
    return &Blob;
}

const TBlobOutput::TStrType TBlobOutput::PutData(const TStringBuf& value)
{
    auto offset = value.size();
    Write(value);
    return TStrType(&Blob, offset, value.size());
}

////////////////////////////////////////////////////////////////////////////////

TFakeStrbufStore::TFakeStrbufStore(size_t /* capacity */)
{ }

const TFakeStrbufStore::TStrType TFakeStrbufStore::PutData(const TStringBuf& value)
{
    return value;
}

void TFakeStrbufStore::Clear()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
