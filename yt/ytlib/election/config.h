#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TCellConfig
    : public TYsonSerializable
{
public:
    //! Cell id.
    TCellGuid CellGuid;

    //! Peer addresses.
    std::vector<Stroka> Addresses;

    TCellConfig()
    {
        RegisterParameter("cell_guid", CellGuid)
            .Default();
        RegisterParameter("addresses", Addresses)
            .NonEmpty();

        RegisterValidator([&] () {
            if (Addresses.size() % 2 != 1) {
                THROW_ERROR_EXCEPTION("Number of peers must be odd");
            }
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
