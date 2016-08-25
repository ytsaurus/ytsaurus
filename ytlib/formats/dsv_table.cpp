#include "dsv_table.h"
#include "config.h"

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TDsvTable::TDsvTable(const TDsvFormatConfigBasePtr& config, bool addCarriageReturn)
{
    std::vector<char> stopSymbols;
    stopSymbols.push_back(config->RecordSeparator);
    stopSymbols.push_back(config->FieldSeparator);
    stopSymbols.push_back(config->EscapingSymbol);
    stopSymbols.push_back('\0');
    if (addCarriageReturn) {
        stopSymbols.push_back('\r');
    }

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
