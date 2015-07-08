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
        auto type = TypeFromId(CellId);
        if (type != EObjectType::ClusterCell && type != EObjectType::TabletCell) {
            THROW_ERROR_EXCEPTION("\"cell_id\" has invalid type %Qlv",
                type);
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

