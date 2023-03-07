#include "routine_registry.h"
#include "private.h"

#include <yt/core/misc/collection_helpers.h>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

// MangleSymbol adds underscore for Darwin platform.
TString MangleSymbol(const TString& name)
{
#ifdef _darwin_
    return "_" + name;
#else
    return name;
#endif
}

// DemangleSymbol strips the prefixed underscore on Darwin,
// returns empty string in case of non-prefixed name.
TString DemangleSymbol(const TString& name)
{
#ifdef _darwin_
    if (name.empty() || name[0] != '_') {
        return TString();
    } else {
        return name.substr(1);
    }
#else
    return name;
#endif
}

////////////////////////////////////////////////////////////////////////////////

uint64_t TRoutineRegistry::GetAddress(const TString& symbol) const
{
    auto it = SymbolToAddress_.find(symbol);
    if (it == SymbolToAddress_.end()) {
        THROW_ERROR_EXCEPTION("Symbol %Qv not found", symbol);
    }
    return it->second;
}

TRoutineRegistry::TValueTypeBuilder TRoutineRegistry::GetTypeBuilder(const TString& symbol) const
{
    return GetOrCrash(SymbolToTypeBuilder_, MangleSymbol(symbol));
}

void TRoutineRegistry::RegisterRoutineImpl(
    const char* symbol,
    uint64_t address,
    TValueTypeBuilder typeBuilder)
{
    auto mangledSymbol = MangleSymbol(symbol);
    YT_VERIFY(SymbolToAddress_.insert(std::make_pair(mangledSymbol, address)).second);
    YT_VERIFY(SymbolToTypeBuilder_.insert(std::make_pair(mangledSymbol, std::move(typeBuilder))).second);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen

