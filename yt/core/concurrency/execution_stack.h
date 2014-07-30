#pragma once

#include "public.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EExecutionStack,
    (Small) // 256 Kb (default)
    (Large) //   8 Mb
);

class TExecutionStackBase
{
public:
    virtual ~TExecutionStackBase();

    void* GetStack() const;
    size_t GetSize() const;

protected:
    void* Stack_;
    size_t Size_;

    explicit TExecutionStackBase(size_t size);

};

#if defined(_unix_)

//! Mapped memory with a few extra guard pages.
class TExecutionStack
    : public TExecutionStackBase
{
public:
    explicit TExecutionStack(size_t size);
    ~TExecutionStack();

private:
    char* Base_;

    static const int GuardPages = 4;

};

#elif defined(_win_)

class TExecutionContext;

//! Stack plus Window fiber holder.
class TExecutionStack
    : public TExecutionStackBase
{
public:
    explicit TExecutionStack(size_t size);
    ~TExecutionStack();

    static void SetOpaque(void* opaque);
    static void* GetOpaque();

    void SetTrampoline(void (*callee)(void*));

private:
    friend class TExecutionContext;

    void* Handle_;
    void (*Trampoline_)(void*);

    static VOID CALLBACK FiberTrampoline(PVOID opaque);

    friend TExecutionContext CreateExecutionContext(
        TExecutionStack* stack,
        void (*trampoline)(void*));

};

#else
#   error Unsupported platform
#endif

std::shared_ptr<TExecutionStack> CreateExecutionStack(EExecutionStack stack);

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NTY

