#include "routine_registry.h"
#include "private.h"

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

// MangleSymbol adds underscore for Darwin platform.
std::string MangleSymbol(std::string name)
{
#ifdef _darwin_
    return "_" + name;
#else
    return name;
#endif
}

// DemangleSymbol strips the prefixed underscore on Darwin,
// returns empty string in case of non-prefixed name.
std::string DemangleSymbol(std::string name)
{
#ifdef _darwin_
    if (name.empty() || name[0] != '_') {
        return std::string();
    } else {
        return name.substr(1);
    }
#else
    return name;
#endif
}

////////////////////////////////////////////////////////////////////////////////

uint64_t TRoutineRegistry::GetAddress(const std::string& symbol) const
{
    auto it = SymbolToAddress_.find(symbol);
    if (it == SymbolToAddress_.end()) {
        THROW_ERROR_EXCEPTION("Symbol %Qv not found", symbol);
    }
    return it->second;
}

TRoutineRegistry::TValueTypeBuilder TRoutineRegistry::GetTypeBuilder(const std::string& symbol) const
{
    return GetOrCrash(SymbolToTypeBuilder_, MangleSymbol(symbol));
}

void TRoutineRegistry::RegisterRoutineImpl(
    const char* symbol,
    uint64_t address,
    TValueTypeBuilder typeBuilder)
{
    auto mangledSymbol = MangleSymbol(symbol);
    YT_VERIFY(SymbolToAddress_.emplace(mangledSymbol, address).second);
    YT_VERIFY(SymbolToTypeBuilder_.emplace(mangledSymbol, std::move(typeBuilder)).second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen
