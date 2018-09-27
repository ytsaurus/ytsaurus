#pragma once

#include "common.h"
#include "ref.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TBlobOutput
    : public IOutputStream
{
public:
    TBlobOutput();
    explicit TBlobOutput(size_t capacity, size_t alignment = 1);

    ~TBlobOutput();

    const TBlob& Blob() const;

    const char* Begin() const;
    size_t Size() const;
    size_t Capacity() const;

    void Reserve(size_t capacity);
    void Clear();
    TSharedRef Flush();

    friend void swap(TBlobOutput& left, TBlobOutput& right);

private:
    virtual void DoWrite(const void* buf, size_t len) override;

    TBlob Blob_;

};

void swap(TBlobOutput& left, TBlobOutput& right);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
