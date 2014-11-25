#pragma once

#include <util/generic/stroka.h>
#include <util/generic/hash.h>

#include <llvm/IR/TypeBuilder.h>

namespace NYT {
namespace NCodegen {

////////////////////////////////////////////////////////////////////////////////

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
            std::bind(&llvm::TypeBuilder<TResult(TArgs...), false>::get, _1));
    }

    uint64_t GetAddress(const Stroka& symbol) const;
    TTypeBuilder GetTypeBuilder(const Stroka& symbol) const;

private:
    void RegisterRoutineImpl(
        const char* symbol,
        uint64_t address,
        TTypeBuilder typeBuilder);

private:
    yhash_map<Stroka, uint64_t> SymbolToAddress_;
    yhash_map<Stroka, TTypeBuilder> SymbolToTypeBuilder_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCodegen
} // namespace NYT

