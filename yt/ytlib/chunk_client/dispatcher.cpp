#include "dispatcher.h"
#include "config.h"

#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>

#include <yt/core/rpc/dispatcher.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    TImpl() = default;

    void Configure(TDispatcherConfigPtr config)
    { }

    void Shutdown()
    {
        ReaderThread_->Shutdown();
        WriterThread_->Shutdown();
    }

    IInvokerPtr GetReaderInvoker()
    {
        return ReaderThread_->GetInvoker();
    }

    IInvokerPtr GetWriterInvoker()
    {
        return WriterThread_->GetInvoker();
    }

private:
    const TActionQueuePtr ReaderThread_ = New<TActionQueue>("ChunkReader");
    const TActionQueuePtr WriterThread_ = New<TActionQueue>("ChunkWriter");
};

TDispatcher::TDispatcher()
    : Impl_(new TImpl())
{ }

TDispatcher::~TDispatcher()
{ }

TDispatcher* TDispatcher::Get()
{
    return Singleton<TDispatcher>();
}

void TDispatcher::StaticShutdown()
{
    Get()->Shutdown();
}

void TDispatcher::Configure(TDispatcherConfigPtr config)
{
    Impl_->Configure(std::move(config));
}

void TDispatcher::Shutdown()
{
    Impl_->Shutdown();
}

IInvokerPtr TDispatcher::GetReaderInvoker()
{
    return Impl_->GetReaderInvoker();
}

IInvokerPtr TDispatcher::GetWriterInvoker()
{
    return Impl_->GetWriterInvoker();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(9, TDispatcher::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
