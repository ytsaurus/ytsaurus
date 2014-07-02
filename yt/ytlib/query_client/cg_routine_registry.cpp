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
    return it != SymbolToAddress_.end() ? it->second : 0;
}

TRoutineRegistry::TTypeBuilder TRoutineRegistry::GetTypeBuilder(const Stroka& symbol) const
{
    return SymbolToTypeBuilder_.at(symbol);
}

bool TRoutineRegistry::RegisterRoutineImpl(
    const char* symbol,
    uint64_t address,
    TTypeBuilder typeBuilder)
{
    YCHECK(SymbolToAddress_.emplace(symbol, std::move(address)).second);
    YCHECK(SymbolToTypeBuilder_.emplace(symbol, std::move(typeBuilder)).second);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

