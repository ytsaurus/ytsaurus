#include "stdafx.h"
#include "cg_routine_registry.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TRoutineRegistry::TRoutineRegistry()
{ }

TRoutineRegistry::~TRoutineRegistry()
{ }

TRoutineRegistry* TRoutineRegistry::Get()
{
    return Singleton<TRoutineRegistry>();
}

uint64_t TRoutineRegistry::GetAddress(const Stroka& symbol) const
{
    auto it = SymbolToAddress_.find(symbol);
    YCHECK(it != SymbolToAddress_.end());
    return it->second;
}

TRoutineRegistry::TTypeBuilder TRoutineRegistry::GetTypeBuilder(const Stroka& symbol) const
{
    auto it = SymbolToTypeBuilder_.find(symbol);
    YCHECK(it != SymbolToTypeBuilder_.end());
    return it->second;
}

bool TRoutineRegistry::RegisterRoutineImpl(
    const char* symbol,
    uint64_t address,
    TTypeBuilder typeBuilder)
{
    YCHECK(SymbolToAddress_.insert(std::make_pair(Stroka(symbol), std::move(address))).second);
    YCHECK(SymbolToTypeBuilder_.insert(std::make_pair(Stroka(symbol), std::move(typeBuilder))).second);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

