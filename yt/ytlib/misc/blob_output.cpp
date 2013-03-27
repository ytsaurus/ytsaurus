#include "stdafx.h"
#include "blob_output.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TBlobOutputTag { };

TBlobOutput::TBlobOutput()
{ }

TBlobOutput::TBlobOutput(size_t capacity)
{
    Reserve(capacity);
}

TBlobOutput::~TBlobOutput() throw()
{ }

void TBlobOutput::DoWrite(const void* buffer, size_t length)
{
    Blob.Append(buffer, length);
}

const char* TBlobOutput::Begin() const
{
    return Blob.Begin();
}

size_t TBlobOutput::GetSize() const
{
    return Blob.Size();
}

void TBlobOutput::Reserve(size_t capacity)
{
    Blob.Reserve(RoundUpToPage(capacity));
}

void TBlobOutput::Clear()
{
    Blob.Clear();
}

TSharedRef TBlobOutput::Flush()
{
    return TSharedRef::FromBlob<TBlobOutputTag>(std::move(Blob));
}

const TBlob& TBlobOutput::GetBlob() const
{
    return Blob;
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
