#pragma once

#include <vector>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

class TMemReader
{
public:
    TMemReader();

    ~TMemReader();

    template <class T>
    bool SafeRead(void* addr, T* value)
    {
        return SafeReadRaw(addr, reinterpret_cast<void*>(value), sizeof(*value));
    }

private:
    int FD_ = -1;

    bool SafeReadRaw(void* addr, void* ptr, size_t size);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
