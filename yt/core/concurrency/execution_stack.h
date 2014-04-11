#pragma once

#include "public.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EExecutionStack,
    (Small) // 256 Kb (default)
    (Large) //   8 Mb
);

class TExecutionStack
{
public:
    TExecutionStack(void* stack, size_t size);

    virtual ~TExecutionStack();

    void* GetStack();
    const size_t GetSize();

protected:
    void* Stack_;
    const size_t Size_;
};

std::shared_ptr<TExecutionStack> CreateExecutionStack(EExecutionStack stack);

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NTY

