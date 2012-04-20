#include "stdafx.h"
#include "blob_output.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TBlobOutput::TBlobOutput()
{ }

TBlobOutput::TBlobOutput(size_t size)
{
    Blob.reserve(size);
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

TSharedRef TBlobOutput::Flush()
{
    return TSharedRef(MoveRV(Blob));
}

void TBlobOutput::Swap(TBlobOutput& other)
{
    Blob.swap(other.Blob);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
