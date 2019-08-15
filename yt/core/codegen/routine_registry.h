#pragma once

#include "type_builder.h"

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

TString MangleSymbol(const TString& name);
TString DemangleSymbol(const TString& name);

template <typename TSignature>
class FunctionTypeBuilder;

template <typename R, typename... Args>
class FunctionTypeBuilder<R(Args...)>
{
public:
    static llvm::FunctionType *get(llvm::LLVMContext &Context) {
        llvm::Type *params[] = {
            TypeBuilder<Args>::get(Context)...
        };
        return llvm::FunctionType::get(TypeBuilder<R>::get(Context),
            params, false);
    }
};

class TRoutineRegistry
{
public:
    typedef std::function<llvm::FunctionType*(llvm::LLVMContext&)> TTypeBuilder;

    template <class TResult, class... TArgs>
    void RegisterRoutine(
        const char* symbol,
        TResult(*fp)(TArgs...))
    {
        using namespace std::placeholders;
        RegisterRoutineImpl(
            symbol,
            reinterpret_cast<uint64_t>(fp),
            std::bind(&FunctionTypeBuilder<TResult(TArgs...)>::get, _1));
    }

    uint64_t GetAddress(const TString& symbol) const;
    TTypeBuilder GetTypeBuilder(const TString& symbol) const;

private:
    void RegisterRoutineImpl(
        const char* symbol,
        uint64_t address,
        TTypeBuilder typeBuilder);

private:
    THashMap<TString, uint64_t> SymbolToAddress_;
    THashMap<TString, TTypeBuilder> SymbolToTypeBuilder_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen

