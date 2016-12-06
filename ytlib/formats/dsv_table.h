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

    TDsvTable(const TDsvFormatConfigBasePtr& config, bool addCarriageReturn);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT

