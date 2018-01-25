#pragma once

#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <llvm/IR/TypeBuilder.h>

namespace NYT {
namespace NCodegen {

////////////////////////////////////////////////////////////////////////////////

TString MangleSymbol(const TString& name);
TString DemangleSymbol(const TString& name);

template <typename TSignature, bool Cross>
class FunctionTypeBuilder;

template <typename R, typename... Args, bool Cross>
class FunctionTypeBuilder<R(Args...), Cross>
{
public:
    static llvm::FunctionType *get(llvm::LLVMContext &Context) {
        llvm::Type *params[] = {
            llvm::TypeBuilder<Args, Cross>::get(Context)...
        };
        return llvm::FunctionType::get(llvm::TypeBuilder<R, Cross>::get(Context),
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
            std::bind(&FunctionTypeBuilder<TResult(TArgs...), false>::get, _1));
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

} // namespace NCodegen
} // namespace NYT

