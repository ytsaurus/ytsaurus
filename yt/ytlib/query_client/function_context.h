#pragma once

#include <vector>
#include <utility>
#include <functional>
#include <memory>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TFunctionContext
{
    explicit TFunctionContext(std::vector<bool> literalArgs);
    ~TFunctionContext();

    // Create local object. Objects are destroyed automaticaly when the function context is destroyed.
    template <class T, class... Args>
    T* CreateObject(Args... args);

    void* GetPrivateData();

    void SetPrivateData(void* data);

    bool IsArgLiteral(int argIndex);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

    void RememberObjectOrDestroy(void* object, void(*deleter)(void*));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#define FUNCTION_CONTEXT_INL_H_
#include "function_context-inl.h"
#undef FUNCTION_CONTEXT_INL_H_

