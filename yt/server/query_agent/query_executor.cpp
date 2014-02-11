#include "query_executor.h"
#include "config.h"
#include "private.h"

#include <ytlib/chunk_client/block_cache.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/schemed_reader.h>
#include <ytlib/new_table_client/schemed_chunk_reader.h>
#include <ytlib/new_table_client/schemed_writer.h>

#include <ytlib/query_client/plan_fragment.h>
#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/executor.h>

#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <server/data_node/block_store.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NQueryAgent {

using namespace NConcurrency;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NVersionedTableClient;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = QueryAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TQueryExecutor
    : public IExecutor
    , public IEvaluateCallbacks
{
public:
    explicit TQueryExecutor(NCellNode::TBootstrap* bootstrap)
        : Bootstrap_(bootstrap)
        , Evaluator_(CreateEvaluator(
            Bootstrap_->GetQueryWorkerInvoker(),
            this))
    { }

    // IExecutor implementation.
    virtual TAsyncError Execute(
        const TPlanFragment& fragment,
        ISchemedWriterPtr writer) override
    {
        LOG_DEBUG("Executing plan fragment (FragmentId: %s)",
            ~ToString(fragment.Id()));
        return Evaluator_->Execute(fragment, std::move(writer));
    }

    // IExecuteCallbacks implementation.
    virtual ISchemedReaderPtr GetReader(
        const TDataSplit& dataSplit,
        TPlanContextPtr context) override
    {
        auto objectId = FromProto<TObjectId>(dataSplit.chunk_id());
        if (TypeFromId(objectId) != EObjectType::Chunk) {
            THROW_ERROR_EXCEPTION("Unsupported data split type %s", 
                ~FormatEnum(TypeFromId(objectId)).Quote());
        }

        LOG_DEBUG("Creating reader for chunk split (ChunkId: %s)",
            ~ToString(objectId));

        auto masterChannel = Bootstrap_->GetMasterChannel();
        auto blockCache = Bootstrap_->GetBlockStore()->GetBlockCache();
        auto nodeDirectory = context->GetNodeDirectory();
        return CreateSchemedChunkReader(
            // TODO(babenko): make configuable
            New<TChunkReaderConfig>(),
            dataSplit,
            std::move(masterChannel),
            std::move(nodeDirectory),
            std::move(blockCache));
    }

private:
    NCellNode::TBootstrap* Bootstrap_;

    IExecutorPtr Evaluator_;

};

IExecutorPtr CreateQueryExecutor(NCellNode::TBootstrap* bootstrap)
{
    return New<TQueryExecutor>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

