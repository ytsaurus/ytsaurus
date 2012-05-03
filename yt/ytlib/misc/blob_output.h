#pragma once

#include "common.h"
#include "ref.h"

namespace NYT {

///////////////////////////////////////////////////////////////////////////////

class TBlobOutput
    : public TOutputStream
{
public:
    TBlobOutput();

    /*!
     * \param size - size of blob reserved in ctor
     */
    TBlobOutput(size_t size);
    ~TBlobOutput() throw();

    void DoWrite(const void* buf, size_t len);

    const TBlob* GetBlob() const;

    const char* Begin() const;
    i32 GetSize() const;

    void Clear();
    TSharedRef Flush(size_t size);

    void Swap(TBlobOutput& other);

private:
    TBlob Blob;
};

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT
