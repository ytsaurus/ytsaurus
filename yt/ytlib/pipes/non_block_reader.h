#pragma once

#include <core/misc/blob.h>
#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NPipes {
namespace NDetail {

// It is not thread-safe
class TNonblockingReader
{
public:
    // It own this fd
    TNonblockingReader(int fd);
    ~TNonblockingReader();

    void ReadToBuffer();
    void Close();

    std::pair<TBlob, bool> GetRead(TBlob&& buffer);

    bool IsBufferFull() const;
    bool IsBufferEmpty() const;

    bool InFailedState() const;
    bool ReachedEOF() const;
    bool IsClosed() const;

    // Returns the last IO error encountered
    // in TryReadInBuffer and Close functions
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

}
}
}
