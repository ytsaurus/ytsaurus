#pragma once

#include <core/misc/blob.h>

#include <core/logging/log.h>

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

    TError Close();

    TErrorOr<size_t> Read(void* buf, size_t len);

    bool IsClosed() const;

private:
    int FD_;

    bool Closed_;

    NLog::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NPipes
} // namespace NYT
