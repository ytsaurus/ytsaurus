#include "stdafx.h"
#include "blob_output.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TBlobOutputTag { };

TBlobOutput::TBlobOutput()
    : Blob_(TBlobOutputTag())
{ }

TBlobOutput::TBlobOutput(size_t capacity)
    : TBlobOutput()
{
    Reserve(capacity);
}

TBlobOutput::~TBlobOutput() throw()
{ }

void TBlobOutput::DoWrite(const void* buffer, size_t length)
{
    Blob_.Append(buffer, length);
}

void TBlobOutput::Reserve(size_t capacity)
{
    Blob_.Reserve(RoundUpToPage(capacity));
}

void TBlobOutput::Clear()
{
    Blob_.Clear();
}

TSharedRef TBlobOutput::Flush()
{
    return TSharedRef::FromBlob(std::move(Blob_));
}

void swap(TBlobOutput& left, TBlobOutput& right)
{
    if (&left != &right) {
        swap(left.Blob_, right.Blob_);
    }
}

const TBlob& TBlobOutput::Blob() const
{
    return Blob_;
}

const char* TBlobOutput::Begin() const
{
    return Blob_.Begin();
}

size_t TBlobOutput::Size() const
{
    return Blob_.Size();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
