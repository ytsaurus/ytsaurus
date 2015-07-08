#include "stdafx.h"
#include "config.h"

#include <ytlib/object_client/helpers.h>

namespace NYT {
namespace NElection {

using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

TCellConfig::TCellConfig()
{
    RegisterParameter("cell_id", CellId);
    RegisterParameter("addresses", Addresses);

    RegisterValidator([&] () {
        if (TypeFromId(CellId) != EObjectType::Cell) {
            THROW_ERROR_EXCEPTION("\"cell_id\" must be a proper id of %Qlv type",
                EObjectType::Cell);
        }
    });
}

void TCellConfig::ValidateAllPeersPresent()
{
    for (int index = 0; index < Addresses.size(); ++index) {
        if (!Addresses[index]) {
            THROW_ERROR_EXCEPTION("Peer %v is missing in configuration of cell %v",
                index,
                CellId);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT

