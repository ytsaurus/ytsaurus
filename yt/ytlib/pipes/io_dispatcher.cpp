#include "stdafx.h"
#include "io_dispatcher.h"
#include "io_dispatcher_impl.h"

namespace NYT {
namespace NPipes {

////////////////////////////////////////////////////////////////////////////////

TIODispatcher::TIODispatcher()
    : Impl_(New<TIODispatcher::TImpl>())
{ }

TIODispatcher::~TIODispatcher()
{ }

TIODispatcher* TIODispatcher::Get()
{
    return Singleton<TIODispatcher>();
}

void TIODispatcher::Shutdown()
{
    return Impl_->Shutdown();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPipes
} // namespace NYT
