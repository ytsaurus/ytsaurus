#include "mem_reader.h"

#include <fcntl.h>
#include <unistd.h>

#include <util/generic/yexception.h>
#include <util/system/error.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

TMemReader::TMemReader()
{
    FD_ = open("/proc/self/mem", O_RDONLY);
    if (FD_ == -1) {
        throw TSystemError(LastSystemError());
    }
}

TMemReader::~TMemReader()
{
    ::close(FD_);
}

bool TMemReader::SafeReadRaw(void* addr, void* ptr, size_t size)
{
    while (true) {
#if defined(_linux_)
        auto ret = pread64(FD_, ptr, size, reinterpret_cast<uintptr_t>(addr));
#else
        auto ret = pread(FD_, ptr, size, reinterpret_cast<uintptr_t>(addr));
#endif
        if (ret == -1 && errno == EINTR) {
            continue;
        }

        return ret == static_cast<int>(size);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
