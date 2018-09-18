#pragma once

#include "public.h"

#include <util/system/context.h>

#include <array>

#if defined(__GNUC__) || defined(__clang__)
#   define CXXABIv1
#endif

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

class TExecutionContext
{
public:
    TExecutionContext() = default;
    TExecutionContext(TExecutionStack* stack, ITrampoLine* trampoline);

    TExecutionContext(const TExecutionContext& other) = delete;
    TExecutionContext& operator=(const TExecutionContext& other) = delete;

    void SwitchTo(TExecutionContext* target);

private:
    TContClosure ContClosure_;
    TContMachineContext ContContext_;
#ifdef CXXABIv1
    static constexpr size_t EHSize = 16;
    std::array<char, EHSize> EH_ = {};
#endif
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NTY

