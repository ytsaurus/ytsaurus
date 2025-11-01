#include "intrinsics.h"

#include <yt/yt/library/web_assembly/api/compartment.h>
#include <yt/yt/library/web_assembly/api/pointer.h>
#include <yt/yt/library/web_assembly/api/type_builder.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/finally.h>

namespace NYT::NWebAssembly {

////////////////////////////////////////////////////////////////////////////////

WAVM_DEFINE_INTRINSIC_MODULE(empty);
WAVM_DEFINE_INTRINSIC_MODULE(standard);

////////////////////////////////////////////////////////////////////////////////

template <class TSignature>
struct TMakeIntrinsic;

template <class TResult, class... TArgs>
struct TMakeIntrinsic<TResult(TArgs...)>
{
    template <TResult(*FunctionPtr)(TArgs...)>
    static TResult Wrapper(WAVM::Runtime::ContextRuntimeData*, TArgs... args)
    {
        auto* compartmentBeforeCall = GetCurrentCompartment();
        auto finally = Finally([&] {
            auto* compartmentAfterCall = GetCurrentCompartment();
            YT_VERIFY(compartmentBeforeCall == compartmentAfterCall);
        });
        return FunctionPtr(args...);
    }
};

#define REGISTER_WEB_ASSEMBLY_INTRINSIC(intrinsic) \
    constexpr auto Intrinsic##intrinsic = &TMakeIntrinsic<decltype(intrinsic)>::Wrapper<&intrinsic>; \
    static WAVM::Intrinsics::Function IntrinsicFunction##intrinsic( \
        NWebAssembly::getIntrinsicModule_standard(), \
        #intrinsic, \
        reinterpret_cast<void*>(Intrinsic##intrinsic), \
        WAVM::IR::FunctionType(WAVM::IR::FunctionType::Encoding{ \
            std::bit_cast<WAVM::Uptr>(NWebAssembly::TFunctionTypeBuilder<true, decltype(intrinsic) >::Get()) \
        }));

#define DEFINE_WEB_ASSEMBLY_SYSCALL_STUB(name, result, ...) \
    result name(__VA_ARGS__) \
    { \
        auto callStack = WAVM::Platform::captureCallStack(1); \
        auto description = WAVM::Runtime::describeCallStack(callStack); \
        auto backtrace = std::string(); \
        int i = 0; \
        for (auto& item : description) { \
            backtrace += std::to_string(i++) + ". "; \
            backtrace += item; \
            backtrace += '\n';\
        } \
        THROW_ERROR_EXCEPTION("WebAssembly call to forbidden system call: %Qv\n\n%v", \
            #name, backtrace); \
    } \
    REGISTER_WEB_ASSEMBLY_INTRINSIC(name);

FOREACH_WEB_ASSEMBLY_SYSCALL(DEFINE_WEB_ASSEMBLY_SYSCALL_STUB);

////////////////////////////////////////////////////////////////////////////////

void emscripten_notify_memory_growth(i64) // NOLINT
{
    // Do nothing.
}
REGISTER_WEB_ASSEMBLY_INTRINSIC(emscripten_notify_memory_growth);

i32 args_sizes_get(i64* argc, i64* argvBufSize)
{
    auto* compartment = GetCurrentCompartment();
    *(PtrFromVM(compartment, argc)) = 1;
    *(PtrFromVM(compartment, argvBufSize)) = 0;
    return 0;
}
REGISTER_WEB_ASSEMBLY_INTRINSIC(args_sizes_get);

i32 environ_sizes_get(i64* argc, i64* argvBufSize)
{
    auto* compartment = GetCurrentCompartment();
    *(PtrFromVM(compartment, argc)) = 1;
    *(PtrFromVM(compartment, argvBufSize)) = 0;
    return 0;
}
REGISTER_WEB_ASSEMBLY_INTRINSIC(environ_sizes_get);

i32 environ_get(i64* first, i64* second)
{
    auto* compartment = GetCurrentCompartment();
    *(PtrFromVM(compartment, first)) = 0;
    *(PtrFromVM(compartment, second)) = 0;
    return 0;
}
REGISTER_WEB_ASSEMBLY_INTRINSIC(environ_get);

void _emscripten_get_progname(char*, i32)
{ }
REGISTER_WEB_ASSEMBLY_INTRINSIC(_emscripten_get_progname);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NWebAssembly
