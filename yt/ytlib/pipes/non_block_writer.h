#pragma once

#include <core/misc/blob.h>
#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NPipes {
namespace NDetail {

class TNonblockingWriter
{
public:
    TNonblockingWriter(int fd);
    ~TNonblockingWriter();

    void WriteFromBuffer();
    void Close();

    void WriteToBuffer(const char* data, size_t size);

    bool IsBufferEmpty() const;
    bool IsBufferFull() const;

    bool IsFailed() const;
    int GetLastSystemError() const;

    bool IsClosed() const;
private:
    int FD_;

    TBlob WriteBuffer_;
    size_t BytesWrittenTotal_;

    bool Closed_;
    int LastSystemError_;

    NLog::TTaggedLogger Logger;

    size_t TryWrite(const char* data, size_t size);
    void TryCleanBuffer();
};

}
}
}
