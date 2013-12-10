#pragma once

#include <core/misc/blob.h>
#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NPipes {
namespace NDetail {

class TNonBlockReader
{
public:
    // It own this fd
    TNonBlockReader(int fd);
    ~TNonBlockReader();

    void TryReadInBuffer();
    std::pair<TBlob, bool> GetRead();

    bool IsBufferFull();
    bool IsBufferEmpty();

    bool InFailedState();
    bool ReachedEOF();
    int GetLastSystemError();

    bool IsReady();

    void Close();
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
