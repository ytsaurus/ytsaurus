#pragma once

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

// Deleter must be drived from TDeleterBase. For raw functions make static variable wrapper.
class TDeleterBase
{
public:
    // TODO: Provide only void* obj. Pointer to deleter can be evaluated from obj.

    typedef void (*TDeleterCallback)(TDeleterBase* deleter, void* ptr);

    explicit TDeleterBase(TDeleterCallback deleter)
        : Delete(deleter)
    { }

    void operator()(void* ptr)
    {
        Delete(this, ptr);
    }

private:
    TDeleterCallback Delete;
};

struct TDefaultDeleter
    : public TDeleterBase
{
    TDefaultDeleter()
        : TDeleterBase(&Free)
    { }

    static void Free(TDeleterBase* /*deleter*/, void* ptr)
    {
        NYTAlloc::Free(ptr);
    }

};

extern TDefaultDeleter DefaultDeleter;

struct TDefaultAllocator
{
    static void* Allocate(size_t size)
    {
        return NYTAlloc::Allocate(size);
    }

    static TDeleterBase* GetDeleter(size_t /*size*/)
    {
        return &DefaultDeleter;
    }

    static void Free(void* ptr)
    {
        NYTAlloc::Free(ptr);
    }
};

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT