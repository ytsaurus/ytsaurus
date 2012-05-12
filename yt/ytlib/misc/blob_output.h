#pragma once

#include "common.h"
#include "ref.h"
#include "blob_range.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

class TBlobOutput
    : public TOutputStream
{
public:
    typedef TBlobRange TStrType;

    TBlobOutput();
    explicit TBlobOutput(size_t capacity);

    ~TBlobOutput() throw();

    void DoWrite(const void* buf, size_t len);

    const TStrType PutData(const TStringBuf& value);

    const TBlob* GetBlob() const;

    const char* Begin() const;
    i32 GetSize() const;

    void Clear();
    TSharedRef Flush(size_t size);

private:
    TBlob Blob;
};

///////////////////////////////////////////////////////////////////////////////

class TFakeStrbufStore
{
public:
    typedef TStringBuf TStrType;

    // Unused parameter, required for compatibility with TBlobOutput.
    TFakeStrbufStore(size_t capacity);

    const TStrType PutData(const TStringBuf& value);

    void Clear();
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
