#pragma once

#include <core/misc/blob.h>

#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NPipes {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TNonblockingWriter
{
public:
    // TODO(babenko): Owns this fd?
    explicit TNonblockingWriter(int fd);
    ~TNonblockingWriter();

    void WriteFromBuffer();
    void Close();

    TErrorOr<size_t> Write(const char* data, size_t size);

    bool IsClosed() const;

private:
    int FD_;

    bool Closed_;

    NLog::TTaggedLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NPipes
} // namespace NYT
