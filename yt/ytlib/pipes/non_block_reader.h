#pragma once

#include <core/misc/blob.h>
#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NPipes {
namespace NDetail {

// It is not thread-safe
class TNonBlockReader
{
public:
    // It own this fd
    TNonBlockReader(int fd);
    ~TNonBlockReader();

    void TryReadInBuffer();
    void Close();

    std::pair<TBlob, bool> GetRead();

    bool IsBufferFull() const;
    bool IsBufferEmpty() const;

    bool InFailedState() const;
    bool ReachedEOF() const;

    // Returns the last IO error encountered
    // in TryReadInBuffer and Close functions
    int GetLastSystemError() const;

    bool IsReady() const;
private:
    int FD;

    TBlob ReadBuffer;
    size_t BytesInBuffer;

    bool ReachedEOF_;
    bool Closed;
    int LastSystemError;

    NLog::TTaggedLogger Logger;
};

}
}
}
