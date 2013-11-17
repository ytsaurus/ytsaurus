#include "stdafx.h"
#include "cypress_ypath_proxy.h"

namespace NYT {
namespace NCypressClient {

using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TStringBuf ObjectIdPathPrefix("#");

TYPath FromObjectId(const TObjectId& id)
{
    return Stroka(ObjectIdPathPrefix) + ToString(id);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressClient
} // namespace NYT

