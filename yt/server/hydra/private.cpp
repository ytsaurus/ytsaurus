#include "private.h"

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/lazy_ptr.h>

namespace NYT {
namespace NHydra {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

const Stroka SnapshotExtension("snapshot");
const Stroka ChangelogExtension("log");
const Stroka ChangelogIndexExtension("index");

IInvokerPtr GetHydraIOInvoker()
{
    static auto queue = New<TActionQueue>("HydraIO");
    return queue->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
