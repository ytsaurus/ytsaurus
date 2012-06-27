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
    typedef TBlobRange TStoredType;

    TBlobOutput();
    explicit TBlobOutput(size_t capacity);

    ~TBlobOutput() throw();

    TStoredType PutData(const TStringBuf& value);

    const TBlob* GetBlob() const;

    const char* Begin() const;
    size_t GetSize() const;

    void Reserve(size_t size);
    void Clear();
    TSharedRef Flush();

private:
    void DoWrite(const void* buf, size_t len);

    TBlob Blob;

};

///////////////////////////////////////////////////////////////////////////////

// TODO(babenko): should we declare this inline?
class TFakeStringBufStore
{
public:
    typedef TStringBuf TStoredType;

    TFakeStringBufStore();
    explicit TFakeStringBufStore(size_t capacity);

    TStoredType PutData(const TStringBuf& value);

    void Clear();
    void Reserve(size_t capacity);
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
