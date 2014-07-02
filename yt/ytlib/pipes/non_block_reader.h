#pragma once

#include <core/misc/blob.h>

#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NPipes {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

// Non thread-safe.
class TNonblockingReader
{
public:
    //! Owns this fd.
    explicit TNonblockingReader(int fd);
    ~TNonblockingReader();

    void ReadToBuffer();
    void Close();

    std::pair<TBlob, bool> GetRead(TBlob&& buffer);

    bool IsBufferFull() const;
    bool IsBufferEmpty() const;

    bool InFailedState() const;
    bool ReachedEOF() const;
    bool IsClosed() const;

    //! Returns the last IO error encountered
    //! in #TryReadInBuffer and #Close functions.
    // TODO(babenko): there's no TryReadInBuffer function.
    // TOD(babenko): the symmetric writer has GetLastSystemError() but no
    // similar comment; please provide it
    int GetLastSystemError() const;

    bool IsReady() const;

private:
    int FD_;

    TBlob ReadBuffer_;
    size_t BytesInBuffer_;

    bool ReachedEOF_;
    bool Closed_;
    int LastSystemError_;

    NLog::TTaggedLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NPipes
} // namespace NYT
