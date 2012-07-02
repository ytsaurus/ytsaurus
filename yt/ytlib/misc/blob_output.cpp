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
    return &*Blob.begin();
}

size_t TBlobOutput::GetSize() const
{
    return Blob.size();
}

void TBlobOutput::Reserve(size_t size)
{
    Blob.reserve(size);
}

void TBlobOutput::Clear()
{
    Blob.clear();
}

TSharedRef TBlobOutput::Flush()
{
    return TSharedRef(MoveRV(Blob));
}

const TBlob* TBlobOutput::GetBlob() const
{
    return &Blob;
}

TBlobOutput::TStoredType TBlobOutput::PutData(const TStringBuf& value)
{
    auto offset = GetSize();
    Write(value);
    return TStoredType(&Blob, offset, value.size());
}

////////////////////////////////////////////////////////////////////////////////

TFakeStringBufStore::TFakeStringBufStore()
{ }

TFakeStringBufStore::TFakeStringBufStore(size_t capacity)
{
    UNUSED(capacity);
}

TFakeStringBufStore::TStoredType TFakeStringBufStore::PutData(const TStringBuf& value)
{
    return value;
}

void TFakeStringBufStore::Clear()
{ }

void TFakeStringBufStore::Reserve(size_t capacity)
{
    UNUSED(capacity);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
