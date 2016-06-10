#include "type_handler.h"

namespace NYT {
namespace NObjectServer {

////////////////////////////////////////////////////////////////////////////////

TObjectBase* IObjectTypeHandler::GetObject(const TObjectId& id)
{
    auto* object = FindObject(id);
    YCHECK(object);
    return object;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjectServer
} // namespace NYT

