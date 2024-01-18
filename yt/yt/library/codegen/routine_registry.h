#pragma once

#include "type_builder.h"

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

TString MangleSymbol(const TString& name);
TString DemangleSymbol(const TString& name);

template <typename TSignature>
class TFunctionTypeBuilder;

template <typename R, typename... Args>
class TFunctionTypeBuilder<R(Args...)>
{
public:
    static llvm::FunctionType *Get(llvm::LLVMContext &Context)
    {
        llvm::Type *params[] = {
            TTypeBuilder<Args>::Get(Context)...
        };
        return llvm::FunctionType::get(TTypeBuilder<R>::Get(Context),
            params, false);
    }
};

template <typename R>
class TFunctionTypeBuilder<R()>
{
public:
    static llvm::FunctionType *Get(llvm::LLVMContext &Context)
    {
        return llvm::FunctionType::get(TTypeBuilder<R>::Get(Context), false);
    }
};

class TRoutineRegistry
{
public:
    using TValueTypeBuilder = std::function<llvm::FunctionType*(llvm::LLVMContext&)>;

    template <class TResult, class... TArgs>
    void RegisterRoutine(
        const char* symbol,
        TResult(*fp)(TArgs...))
    {
        using namespace std::placeholders;
        RegisterRoutineImpl(
            symbol,
            reinterpret_cast<uint64_t>(fp),
            std::bind(&TFunctionTypeBuilder<TResult(TArgs...)>::Get, _1));
    }

    uint64_t GetAddress(const TString& symbol) const;
    TValueTypeBuilder GetTypeBuilder(const TString& symbol) const;

private:
    void RegisterRoutineImpl(
        const char* symbol,
        uint64_t address,
        TValueTypeBuilder typeBuilder);

private:
    THashMap<TString, uint64_t> SymbolToAddress_;
    THashMap<TString, TValueTypeBuilder> SymbolToTypeBuilder_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
