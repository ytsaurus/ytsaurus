#pragma once

#include "common.h"
#include "ref.h"

#include <util/stream/output.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TChunkedOutputStream
    : public IOutputStream
{
public:
    TChunkedOutputStream(
        TRefCountedTypeCookie tagCookie,
        size_t initialReserveSize,
        size_t maxReserveSize);

    template <class TTag>
    explicit TChunkedOutputStream(
        TTag tag = TTag(),
        size_t initialReserveSize = 4 * 1024,
        size_t maxReserveSize = 64 * 1024)
        : TChunkedOutputStream(GetRefCountedTypeCookie<TTag>(), initialReserveSize, maxReserveSize)
    { }

    TChunkedOutputStream();

    TChunkedOutputStream(TChunkedOutputStream&&) = default;
    TChunkedOutputStream& operator=(TChunkedOutputStream&&) = default;

    //! Remind user about the tag argument.
    template <typename U> TChunkedOutputStream(i32, U size = 0) = delete;
    template <typename U> TChunkedOutputStream(i64, U size = 0) = delete;
    template <typename U> TChunkedOutputStream(ui32, U size = 0) = delete;
    template <typename U> TChunkedOutputStream(ui64, U size = 0) = delete;

    ~TChunkedOutputStream() throw() = default;

    //! Returns a sequence of written chunks.
    //! The stream is no longer usable after this call.
    std::vector<TSharedRef> Flush();

    //! Returns the number of bytes actually written.
    size_t GetSize() const;

    //! Returns the number of bytes actually written plus unused capacity in the
    //! last chunk.
    size_t GetCapacity() const;

    //! Returns a pointer to a contiguous memory block of a given #size.
    //! Do not forget to call #Advance after use.
    char* Preallocate(size_t size);

    //! Marks #size bytes (which were previously preallocated) as used.
    void Advance(size_t size);

private:
    size_t MaxReserveSize_;
    size_t CurrentReserveSize_;

    size_t FinishedSize_ = 0;

    TBlob CurrentChunk_;
    std::vector<TSharedRef> FinishedChunks_;

    virtual void DoWrite(const void* buf, size_t len) override;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
