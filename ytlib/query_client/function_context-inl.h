#ifndef FUNCTION_CONTEXT_INL_H_
#error "Direct inclusion of this file is not allowed, include function_context.h"
#endif

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

template <class T, class... Args>
T* TFunctionContext::CreateObject(Args... args)
{
    T* objectPtr = new T(args...);

    RememberObjectOrDestroy(objectPtr, [] (void* ptr) { static_cast<T*>(ptr)->~T(); });

    return objectPtr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT   
