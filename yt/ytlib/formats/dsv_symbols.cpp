#include "dsv_symbols.h"
#include "config.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

// It is used instead functions for optimization dsv performance
char EscapingTable[256] = {};
char UnEscapingTable[256] = {};
bool IsKeyStopSymbol[256] = {};
bool IsValueStopSymbol[256] = {};

void InitDsvSymbols(const TDsvFormatConfigPtr& config)
{
    IsKeyStopSymbol[config->RecordSeparator] = true;
    IsKeyStopSymbol[config->FieldSeparator] = true;
    IsKeyStopSymbol[config->KeyValueSeparator] = true;
    IsKeyStopSymbol[config->EscapingSymbol] = true;
    IsKeyStopSymbol['\0'] = true;

    IsValueStopSymbol[config->RecordSeparator] = true;
    IsValueStopSymbol[config->FieldSeparator] = true;
    IsValueStopSymbol[config->EscapingSymbol] = true;
    IsValueStopSymbol['\0'] = true;

    // Init escaping table
    for (int i = 0; i < 256; ++i) {
        EscapingTable[i] = i;
    }
    EscapingTable['\0'] = '0';
    EscapingTable['\n'] = 'n';
    EscapingTable['\t'] = 't';

    for (int i = 0; i < 256; ++i) {
        UnEscapingTable[i] = i;
    }
    UnEscapingTable['0'] = '\0';
    UnEscapingTable['t'] = '\t';
    UnEscapingTable['n'] = '\n';
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
