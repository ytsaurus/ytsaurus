#pragma once

#include "cg_types.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TRoutineRegistry
{
public:
    typedef std::function<llvm::FunctionType*(llvm::LLVMContext&)> TTypeBuilder;

    static TRoutineRegistry* Get();

    template <class TResult, class... TArgs>
    static bool RegisterRoutine(
        const char* symbol,
        TResult(*fp)(TArgs...))
    {
        using namespace std::placeholders;
        return Get()->RegisterRoutineImpl(
            symbol,
            reinterpret_cast<uint64_t>(fp),
            std::bind(&llvm::TypeBuilder<TResult(TArgs...), false>::get, _1));
    }

    uint64_t GetAddress(const Stroka& symbol) const;

    TTypeBuilder GetTypeBuilder(const Stroka& symbol) const;

private:
    TRoutineRegistry();
    ~TRoutineRegistry();

    DECLARE_SINGLETON_FRIEND(TRoutineRegistry);

    bool RegisterRoutineImpl(
        const char* symbol,
        uint64_t address,
        TTypeBuilder typeBuilder);

private:
    yhash_map<Stroka, uint64_t> SymbolToAddress_;
    yhash_map<Stroka, TTypeBuilder> SymbolToTypeBuilder_;

    static Stroka MangleSymbol(const Stroka& name);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

