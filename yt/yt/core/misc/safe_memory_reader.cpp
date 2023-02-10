#include "safe_memory_reader.h"

#include "error.h"
#include "proc.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#ifdef _win_

TSafeMemoryReader::TSafeMemoryReader()
{
    Y_UNUSED(FD_);
    THROW_ERROR_EXCEPTION("Reading process memory is not supported for this platform");
}

TSafeMemoryReader::~TSafeMemoryReader() = default;

bool TSafeMemoryReader::ReadRaw(const void* /*addr*/, void* /*ptr*/, size_t /*size*/)
{
    return false;
}

#else

TSafeMemoryReader::TSafeMemoryReader()
{
    FD_ = ::open("/proc/self/mem", O_RDONLY);
    if (FD_ == -1) {
        THROW_ERROR_EXCEPTION("Failed to open process memory for reading")
            << TError::FromSystem();
    }
}

TSafeMemoryReader::~TSafeMemoryReader()
{
    if (FD_ != -1) {
        ::close(FD_);
    }
}

bool TSafeMemoryReader::ReadRaw(const void* addr, void* ptr, size_t size)
{
#if defined(_linux_)
    auto ret = HandleEintr(::pread64, FD_, ptr, size, reinterpret_cast<uintptr_t>(addr));
#else
    auto ret = HandleEintr(::pread, FD_, ptr, size, reinterpret_cast<uintptr_t>(addr));
#endif
    return ret == static_cast<int>(size);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
