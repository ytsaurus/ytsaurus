#include "dsv_symbols.h"
#include "config.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TDsvSymbolTable::TDsvSymbolTable(TDsvFormatConfigPtr config)
{
    YCHECK(config);

    for (int i = 0; i < 256; ++i) {
        EscapingTable[i] = i;
        UnescapingTable[i] = i;
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

    UnescapingTable['0'] = '\0';
    UnescapingTable['t'] = '\t';
    UnescapingTable['n'] = '\n';
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
