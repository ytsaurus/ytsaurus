#pragma once

#include "public.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TDsvSymbolTable
{
    explicit TDsvSymbolTable(TDsvFormatConfigPtr config);

    char EscapingTable[256];
    char UnescapingTable[256];
    bool IsKeyStopSymbol[256];
    bool IsValueStopSymbol[256];
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
