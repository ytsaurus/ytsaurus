#pragma once

#include <util/stream/zerocopy_output.h>

#include "common.h"
#include "ref.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TBlobOutput
    : public IZeroCopyOutput
{
public:
    TBlobOutput();
    explicit TBlobOutput(size_t capacity, size_t alignment = 1);

    ~TBlobOutput();

    TBlob& Blob();
    const TBlob& Blob() const;

    const char* Begin() const;
    size_t Size() const;
    size_t Capacity() const;

    void Reserve(size_t capacity);
    void Clear();
    TSharedRef Flush();

    friend void swap(TBlobOutput& left, TBlobOutput& right);

private:
    virtual size_t DoNext(void** ptr) override;
    virtual void DoUndo(size_t len) override;
    virtual void DoWrite(const void* buf, size_t len) override;

    TBlob Blob_;
};

void swap(TBlobOutput& left, TBlobOutput& right);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
