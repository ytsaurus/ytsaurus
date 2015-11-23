#include "stdafx.h"
#include "private.h"

#include <core/misc/lazy_ptr.h>

#include <core/concurrency/action_queue.h>

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
