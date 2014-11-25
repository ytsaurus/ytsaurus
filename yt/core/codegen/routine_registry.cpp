#include "stdafx.h"
#include "private.h"
#include "routine_registry.h"

namespace NYT {
namespace NCodegen {

////////////////////////////////////////////////////////////////////////////////

static Stroka MangleSymbol(const Stroka& name)
{
#ifdef _darwin_
    return "_" + name;
#else
    return name;
#endif
}

////////////////////////////////////////////////////////////////////////////////

uint64_t TRoutineRegistry::GetAddress(const Stroka& symbol) const
{
    auto it = SymbolToAddress_.find(symbol);
    YCHECK(it != SymbolToAddress_.end());
    return it->second;
}

TRoutineRegistry::TTypeBuilder TRoutineRegistry::GetTypeBuilder(const Stroka& symbol) const
{
    auto mangledSymbol = MangleSymbol(symbol);
    auto it = SymbolToTypeBuilder_.find(mangledSymbol);
    YCHECK(it != SymbolToTypeBuilder_.end());
    return it->second;
}

void TRoutineRegistry::RegisterRoutineImpl(
    const char* symbol,
    uint64_t address,
    TTypeBuilder typeBuilder)
{
    auto mangledSymbol = MangleSymbol(symbol);
    YCHECK(SymbolToAddress_.insert(std::make_pair(mangledSymbol, std::move(address))).second);
    YCHECK(SymbolToTypeBuilder_.insert(std::make_pair(mangledSymbol, std::move(typeBuilder))).second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCodegen
} // namespace NYT

