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
    for (int i = 0; i < 256;; ++i) {
        EscapingTable[i] = i;
        UnEscapingTable[i] = i;
        IsKeyStopSymbol[i] = false;
        IsValueStopSymbol[i] = false;
    }

    IsKeyStopSymbol[static_cast<unsigned char>(config->RecordSeparator)] = true;
    IsKeyStopSymbol[static_cast<unsigned char>(config->FieldSeparator)] = true;
    IsKeyStopSymbol[static_cast<unsigned char>(config->KeyValueSeparator)] = true;
    IsKeyStopSymbol[static_cast<unsigned char>(config->EscapingSymbol)] = true;
    IsKeyStopSymbol[static_cast<unsigned char>('\0')] = true;

    IsValueStopSymbol[static_cast<unsigned char>(config->RecordSeparator)] = true;
    IsValueStopSymbol[static_cast<unsigned char>(config->FieldSeparator)] = true;
    IsValueStopSymbol[static_cast<unsigned char>(config->EscapingSymbol)] = true;
    IsValueStopSymbol[static_cast<unsigned char>('\0')] = true;

    EscapingTable['\0'] = '0';
    EscapingTable['\n'] = 'n';
    EscapingTable['\t'] = 't';

    UnEscapingTable['0'] = '\0';
    UnEscapingTable['t'] = '\t';
    UnEscapingTable['n'] = '\n';
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
