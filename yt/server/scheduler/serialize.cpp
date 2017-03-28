#include "serialize.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

int GetCurrentSnapshotVersion()
{
    return 200009;
}

bool ValidateSnapshotVersion(int version)
{
    return
        version == 200005 ||
    	version == 200006 ||
    	version == 200007 ||
    	version == 200008 ||
    	version == 200009;
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

