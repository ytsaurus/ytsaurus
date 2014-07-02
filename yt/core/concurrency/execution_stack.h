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
    virtual ~TExecutionStack();

    void* GetStack() const;
    size_t GetSize() const;

protected:
    void* Stack_;
    size_t Size_;

    explicit TExecutionStack(size_t size);

};

std::shared_ptr<TExecutionStack> CreateExecutionStack(EExecutionStack stack);

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NTY

