#pragma once

#include <memory>
#include <utility>
#include <vector>

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
    std::vector<std::unique_ptr<void, void(*)(void*)>> Objects_;
    std::vector<bool> LiteralArgs_;
    void* PrivateData_ = nullptr;

    void RememberObjectOrDestroy(void* object, void(*deleter)(void*));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

#define FUNCTION_CONTEXT_INL_H_
#include "function_context-inl.h"
#undef FUNCTION_CONTEXT_INL_H_

