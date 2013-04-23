#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EUpdateMode,
    (None)
    (Append)
    (Overwrite)
);

/*
To be done:

class TChunkOwnerYPathProxy
*/

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
