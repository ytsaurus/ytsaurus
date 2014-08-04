#include "stdafx.h"
#include "cg_routine_registry.h"

#include <core/misc/singleton.h>

namespace NYT {
namespace NQueryClient {

static Stroka DemangleSymbol(const Stroka& symbol)
{
#ifdef _darwin_
    if (symbol.length() > 0 && symbol[0] == '_') {
        return symbol.substr(1);
    }
#endif
    return symbol;
}

////////////////////////////////////////////////////////////////////////////////

TRoutineRegistry::TRoutineRegistry()
{ }

TRoutineRegistry::~TRoutineRegistry()
{ }

TRoutineRegistry* TRoutineRegistry::Get()
{
    return TSingleton::Get();
}

uint64_t TRoutineRegistry::GetAddress(const Stroka& symbol) const
{
    auto it = SymbolToAddress_.find(DemangleSymbol(symbol));
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

bool TRoutineRegistry::RegisterRoutineImpl(
    const char* symbol,
    uint64_t address,
    TTypeBuilder typeBuilder)
{
    auto mangledSymbol = MangleSymbol(symbol);
    YCHECK(SymbolToAddress_.insert(std::make_pair(mangledSymbol, std::move(address))).second);
    YCHECK(SymbolToTypeBuilder_.insert(std::make_pair(mangledSymbol, std::move(typeBuilder))).second);
    return true;
}

Stroka TRoutineRegistry::MangleSymbol(const Stroka& name)
{
#ifdef _darwin_
    return "_" + name;
#else
    return name;
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

