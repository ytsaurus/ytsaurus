#include "admin.h"
#include "box.h"
#include "config.h"
#include "connection.h"
#include "private.h"

#include <yt/ytlib/api/admin.h>

#include <yt/ytlib/admin/admin_service_proxy.h>

#include <yt/ytlib/hive/cell_directory.h>
#include <yt/ytlib/hive/cell_directory_synchronizer.h>

#include <yt/ytlib/hydra/hydra_service_proxy.h>

#include <yt/ytlib/node_tracker_client/node_tracker_service_proxy.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/concurrency/scheduler.h>

namespace NYT {
namespace NApi {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

using namespace NAdmin;
using namespace NConcurrency;
using namespace NRpc;
using namespace NObjectClient;
using namespace NTabletClient;
using namespace NNodeTrackerClient;
using namespace NHydra;
using namespace NHiveClient;

DECLARE_REFCOUNTED_CLASS(TAdmin)

////////////////////////////////////////////////////////////////////////////////

class TAdmin
    : public IAdmin
{
public:
    TAdmin(
        IConnectionPtr connection,
        const TAdminOptions& options)
        : Connection_(std::move(connection))
        , Options_(options)
        , Logger(NLogging::TLogger(ApiLogger)
            .AddTag("AdminId: %v", TGuid::Create()))
    {
        Y_UNUSED(Options_);
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

    IMPLEMENT_METHOD(int, BuildSnapshot, (
        const TBuildSnapshotOptions& options),
        (options))
    IMPLEMENT_METHOD(void, GCCollect, (
        const TGCCollectOptions& options),
        (options))
    IMPLEMENT_METHOD(void, KillProcess, (
        const TString& address,
        const TKillProcessOptions& options),
        (address, options))
    IMPLEMENT_METHOD(TString, WriteCoreDump, (
        const TString& address,
        const TWriteCoreDumpOptions& options),
        (address, options))

private:
    const IConnectionPtr Connection_;
    const TAdminOptions Options_;

    const NLogging::TLogger Logger;


    template <class T>
    TFuture<T> Execute(const TString& commandName, TCallback<T()> callback)
    {
        return BIND([=, this_ = MakeStrong(this)] () {
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
            .AsyncVia(Connection_->GetInvoker())
            .Run();
    }

    int DoBuildSnapshot(const TBuildSnapshotOptions& options)
    {
        auto cellId = options.CellId ? options.CellId : Connection_->GetPrimaryMasterCellId();
        auto channel = GetCellChannelOrThrow(cellId);

        THydraServiceProxy proxy(channel);
        auto req = proxy.ForceBuildSnapshot();
        req->SetTimeout(TDuration::Hours(1)); // effective infinity
        req->set_set_read_only(options.SetReadOnly);

        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();

        return rsp->snapshot_id();
    }

    void DoGCCollect(const TGCCollectOptions& options)
    {
        auto cellId = options.CellId ? options.CellId : Connection_->GetPrimaryMasterCellId();
        auto channel = Connection_->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellId);

        TObjectServiceProxy proxy(channel);
        auto req = proxy.GCCollect();
        req->SetTimeout(TDuration::Hours(1)); // effective infinity

        WaitFor(req->Invoke())
            .ThrowOnError();
    }

    void DoKillProcess(const TString& address, const TKillProcessOptions& options)
    {
        auto channel = Connection_->GetChannelFactory()->CreateChannel(address);

        TAdminServiceProxy proxy(channel);
        auto req = proxy.Die();
        req->set_exit_code(options.ExitCode);
        auto asyncResult = req->Invoke().As<void>();
        // NB: this will always throw an error since the service can
        // never reply to the request because it makes _exit immediately.
        // This is the intended behavior.
        WaitFor(asyncResult)
            .ThrowOnError();
    }

    TString DoWriteCoreDump(const TString& address, const TWriteCoreDumpOptions& options)
    {
        auto channel = Connection_->GetChannelFactory()->CreateChannel(address);

        TAdminServiceProxy proxy(channel);
        auto req = proxy.WriteCoreDump();
        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();
        return rsp->path();
    }


    IChannelPtr GetCellChannelOrThrow(const TCellId& cellId)
    {
        const auto& cellDirectory = Connection_->GetCellDirectory();
        auto channel = cellDirectory->FindChannel(cellId);
        if (channel) {
            return channel;
        }

        WaitFor(Connection_->GetCellDirectorySynchronizer()->Sync())
            .ThrowOnError();

       return cellDirectory->GetChannelOrThrow(cellId);
    }
};

DEFINE_REFCOUNTED_TYPE(TAdmin)

IAdminPtr CreateAdmin(
    IConnectionPtr connection,
    const TAdminOptions& options)
{
    return New<TAdmin>(std::move(connection), options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NApi
} // namespace NYT
