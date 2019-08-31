#include "resource_helpers.h"

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

bool IsHomogeneous(EResourceKind kind)
{
    return
        kind == EResourceKind::Cpu ||
        kind == EResourceKind::Memory ||
        kind == EResourceKind::Network ||
        kind == EResourceKind::Slot;
}

/////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
