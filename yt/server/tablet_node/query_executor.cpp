#include "query_executor.h"
#include "store_manager.h"
#include "tablet.h"
#include "tablet_manager.h"

#include <core/concurrency/fiber.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/executor.h>
#include <ytlib/query_client/helpers.h>

#include <ytlib/object_client/helpers.h>

#include <server/query_agent/private.h>

namespace NYT {
namespace NTabletNode {

using namespace NConcurrency;
using namespace NQueryClient;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = NQueryAgent::QueryAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TQueryExecutor
    : public IExecutor
    , public IEvaluateCallbacks
{
public:
    TQueryExecutor(
        IInvokerPtr automatonInvoker,
        IInvokerPtr workerInvoker,
        TTabletManagerPtr tabletManager)
        : AutomatonInvoker_(automatonInvoker)
        , WorkerInvoker_(workerInvoker)
        , TabletManager_(tabletManager)
        , Evaluator_(CreateEvaluator(WorkerInvoker_, this))
    { }

    // IExecutor implementation.
    virtual TAsyncError Execute(
        const TPlanFragment& fragment,
        ISchemedWriterPtr writer) override
    {
        LOG_DEBUG("Executing plan fragment (FragmentId: %s)",
            ~ToString(fragment.Id()));
        
        return Evaluator_->Execute(
            fragment,
            std::move(writer));
    }

    // IExecuteCallbacks implementation.
    virtual ISchemedReaderPtr GetReader(
        const TDataSplit& split,
        TPlanContextPtr context) override
    {
        auto asyncResult = BIND(&TQueryExecutor::DoGetReader, MakeStrong(this))
            .Guarded()
            .AsyncVia(AutomatonInvoker_)
            .Run(split, std::move(context));
        auto result = WaitFor(asyncResult);
        THROW_ERROR_EXCEPTION_IF_FAILED(result);
        return result.GetValue();
    }

private:
    IInvokerPtr AutomatonInvoker_;
    IInvokerPtr WorkerInvoker_;
    TTabletManagerPtr TabletManager_;

    IExecutorPtr Evaluator_;


    ISchemedReaderPtr DoGetReader(
        const TDataSplit& split,
        TPlanContextPtr /*context*/)
    {
        auto objectId = FromProto<TObjectId>(split.chunk_id());
        if (TypeFromId(objectId) != EObjectType::Tablet) {
            THROW_ERROR_EXCEPTION("Unsupported data split type %s", 
                ~FormatEnum(TypeFromId(objectId)).Quote());
        }

        LOG_DEBUG("Creating reader for tablet split (TabletId: %s)",
            ~ToString(objectId));

        auto* tablet = TabletManager_->GetTabletOrThrow(objectId);
        const auto& storeManager = tablet->GetStoreManager();
        auto lowerBound = GetLowerBoundFromDataSplit(split);
        auto upperBound = GetUpperBoundFromDataSplit(split);
        // TODO(babenko): timestamp
        return storeManager->CreateReader(
            std::move(lowerBound),
            std::move(upperBound),
            NVersionedTableClient::LastCommittedTimestamp);
    }

};

IExecutorPtr CreateQueryExecutor(
    IInvokerPtr automatonInvoker,
    IInvokerPtr workerInvoker,
    TTabletManagerPtr tabletManager)
{
    return New<TQueryExecutor>(
        automatonInvoker,
        workerInvoker,
        tabletManager);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT

