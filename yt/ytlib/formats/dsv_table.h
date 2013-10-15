#pragma once

#include "public.h"
#include "symbols.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

struct TDsvTable
{
    TLookupTable KeyStops;
    TLookupTable ValueStops;
    TEscapeTable Escapes;

    TDsvTable(const TDsvFormatConfigPtr& config)
    {
        std::vector<char> stopSymbols;
        stopSymbols.push_back(config->RecordSeparator);
        stopSymbols.push_back(config->FieldSeparator);
        stopSymbols.push_back(config->EscapingSymbol);
        stopSymbols.push_back('\0');
        stopSymbols.push_back('\r');

        std::vector<char> valueStopSymbols(stopSymbols);
        ValueStops.Fill(
            valueStopSymbols.data(),
            valueStopSymbols.data() + valueStopSymbols.size());
        
        std::vector<char> keyStopSymbols(stopSymbols);
        keyStopSymbols.push_back(config->KeyValueSeparator);
        KeyStops.Fill(
            keyStopSymbols.data(),
            keyStopSymbols.data() + keyStopSymbols.size());
    }
};

} // namespace NFormats
} // namespace NYT

