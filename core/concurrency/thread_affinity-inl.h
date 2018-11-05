#pragma once
#ifndef THREAD_AFFINITY_INL_H_
#error "Direct inclusion of this file is not allowed, include thread_affinity.h"
// For the sake of sane code completion.
#include "thread_affinity.h"
#include "thread_affinity.h"
#endif
#undef THREAD_AFFINITY_INL_H_

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// Forward declaration.
bool VerifyInvokerAffinity(const IInvokerPtr& invoker);

template <class T>
bool VerifyInvokersAffinity(const T& invokers)
{
    for (const auto& invoker : invokers) {
        if (VerifyInvokerAffinity(invoker)) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
