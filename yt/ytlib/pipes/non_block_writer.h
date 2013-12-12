#pragma once

#include <core/misc/blob.h>
#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NPipes {
namespace NDetail {

class TNonBlockWriter
{
public:
    TNonBlockWriter(int fd);
    ~TNonBlockWriter();

    void TryWriteFromBuffer();
    void Close();

    void WriteToBuffer(const char* data, size_t size);

    bool IsBufferEmpty() const;
    bool IsBufferFull() const;

    bool InFailedState() const;
    int GetLastSystemError() const;

    bool IsClosed() const;
private:
    int FD;

    TBlob WriteBuffer;
    size_t BytesWrittenTotal;

    bool Closed;
    int LastSystemError;

    NLog::TTaggedLogger Logger;

    size_t TryWrite(const char* data, size_t size);
    void TryCleanBuffer();
};

}
}
}
