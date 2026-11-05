#include "state_provider.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void IMutableStateKeyProvider::EraseKeyState(const TKey& key)
{
    THROW_ERROR_EXCEPTION("Erasing a key state without loading it is not supported by this store")
        .With("key", key);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
