#include "stdafx.h"
#include "admin.h"
#include "connection.h"
#include "config.h"
#include "box.h"
#include "private.h"

#include <core/concurrency/scheduler.h>

#include <ytlib/driver/dispatcher.h>

#include <ytlib/object_client/object_service_proxy.h>

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NRpc;

DECLARE_REFCOUNTED_CLASS(TAdmin)

////////////////////////////////////////////////////////////////////////////////

class TAdmin
    : public IAdmin
{
public:
    TAdmin(IConnectionPtr connection, const TAdminOptions& options)
        : Connection_(std::move(connection))
        , Options_(options)
        , Invoker_(NDriver::TDispatcher::Get()->GetLightInvoker())
        , LeaderChannel_(Connection_->GetMasterChannel(EMasterChannelKind::Leader))
    {
        Logger.AddTag("Admin: %p", this);
    }

#define DROP_BRACES(...) __VA_ARGS__
#define IMPLEMENT_METHOD(returnType, method, signature, args) \
    virtual TFuture<returnType> method signature override \
    { \
        return Execute( \
            #method, \
            BIND( \
                &TAdmin::Do ## method, \
                MakeStrong(this), \
                DROP_BRACES args)); \
    }

    IMPLEMENT_METHOD(int, BuildSnapshot, (const TBuildSnapshotOptions& options), (options))

    IMPLEMENT_METHOD(void, GCCollect, (const TGCCollectOptions& options), (options))

private:
    const IConnectionPtr Connection_;
    const TAdminOptions Options_;

    const IInvokerPtr Invoker_;

    const IChannelPtr LeaderChannel_;

    NLog::TLogger Logger = ApiLogger;


    template <class T>
    TFuture<T> Execute(const Stroka& commandName, TCallback<T()> callback)
    {
        auto this_ = MakeStrong(this);
        return
            BIND([=] () {
                UNUSED(this_);
                try {
                    LOG_DEBUG("Command started (Command: %v)", commandName);
                    TBox<T> result(callback);
                    LOG_DEBUG("Command completed (Command: %v)", commandName);
                    return result.Unwrap();
                } catch (const std::exception& ex) {
                    LOG_DEBUG(ex, "Command failed (Command: %v)", commandName);
                    throw;
                }
            })
            .AsyncVia(Invoker_)
            .Run();
    }

    int DoBuildSnapshot(const TBuildSnapshotOptions& options)
    {
        TObjectServiceProxy proxy(LeaderChannel_);
        proxy.SetDefaultTimeout(Null); // infinity

        auto req = proxy.BuildSnapshot();
        req->set_set_read_only(options.SetReadOnly);

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        return rsp->snapshot_id();
    }

    void DoGCCollect(const TGCCollectOptions& /*options*/)
    {
        TObjectServiceProxy proxy(LeaderChannel_);
        proxy.SetDefaultTimeout(Null); // infinity

        auto req = proxy.GCCollect();
        WaitFor(req->Invoke())
            .ThrowOnError();
    }
};

DEFINE_REFCOUNTED_TYPE(TAdmin)

IAdminPtr CreateAdmin(IConnectionPtr connection, const TAdminOptions& options)
{
    return New<TAdmin>(std::move(connection), options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT
