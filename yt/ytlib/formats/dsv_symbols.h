#pragma once

#include "public.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

extern char EscapingTable[256];
extern char UnEscapingTable[256];
extern bool IsKeyStopSymbol[256];
extern bool IsValueStopSymbol[256];

void InitDsvSymbols(const TDsvFormatConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
