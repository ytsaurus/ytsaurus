#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/value_consumer.h>
#include <yt/yt/client/table_client/table_consumer.h>
#include <yt/yt/ytlib/table_client/helpers.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/library/formats/format.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <yt/yt/core/actions/callback.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/invoker_queue.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler_thread.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/mpsc_stack.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/build/build.h>

#include <util/stream/printf.h>
#include <util/string/cast.h>
#include <util/system/file.h>

#include <yt/yt/ytlib/transaction_supervisor/transaction_participant_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/helpers.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/helpers.h>

#include <yt/yt/core/bus/tcp/config.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <mutex>
#include <iostream>
#include <signal.h>
#include <thread>

using namespace NYT;
using namespace NApi;
using namespace NConcurrency;
using namespace NFormats;
using namespace NTableClient;
using namespace NTracing;
using namespace NTransactionClient;
using namespace NTabletClient;
using namespace NYTree;
using namespace NYson;
using namespace NHiveClient;
using namespace NTransactionSupervisor;

int main(int /*argc*/, char* argv[])
{
    try {
        auto addr = TString(argv[1]);
        auto cellId = TGuid::FromString(TString(argv[2]));
        auto txId = TGuid::FromString(TString(argv[3]));

        auto channel = NRpc::CreateRealmChannel(
            NRpc::NBus::CreateTcpBusChannelFactory(New<NBus::TBusConfig>())->CreateChannel(addr),
            cellId);


        TTransactionParticipantServiceProxy proxy(channel);
        auto req = proxy.CommitTransaction();
        ToProto(req->mutable_transaction_id(), txId);
        req->set_commit_timestamp(TimestampFromTransactionId(txId) + 100);

        WaitFor(req->Invoke()).ValueOrThrow();
    } catch (std::exception& e) {
        Cerr << ToString(TError(e)) << Endl;
    }

    return 0;
}
