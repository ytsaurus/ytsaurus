#include "query_manager.h"

#include "config.h"
#include "private.h"

#include <core/concurrency/action_queue.h>

#include <ytlib/chunk_client/block_cache.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/schemed_reader.h>
#include <ytlib/new_table_client/schemed_chunk_reader.h>
#include <ytlib/new_table_client/schemed_writer.h>

#include <ytlib/query_client/plan_fragment.h>

#include <ytlib/chunk_client/chunk_spec.pb.h>

#include <server/data_node/block_store.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NQueryAgent {

using namespace NCellNode;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NVersionedTableClient;
using namespace NNodeTrackerClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = QueryAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TQueryManager::TQueryManager(
    TQueryAgentConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , WorkerPool_(New<TThreadPool>(Config_->PoolSize, "Query"))
{
    Evaluator_ = CreateEvaluator(WorkerPool_->GetInvoker(), this);
}

TQueryManager::~TQueryManager()
{ }

TAsyncError TQueryManager::Execute(
    const TPlanFragment& fragment,
    ISchemedWriterPtr writer)
{
    LOG_DEBUG("Executing plan fragment %s", ~ToString(fragment.Id()));
    return Evaluator_->Execute(fragment, std::move(writer));
}

ISchemedReaderPtr TQueryManager::GetReader(
    const TDataSplit& dataSplit,
    TPlanContextPtr context)
{
    auto masterChannel = Bootstrap_->GetMasterChannel();
    auto blockCache = Bootstrap_->GetBlockStore()->GetBlockCache();
    auto nodeDirectory = context->GetNodeDirectory();

    auto objectId = FromProto<TObjectId>(dataSplit.chunk_id());
    LOG_DEBUG("Creating reader for %s", ~ToString(objectId));

    switch (TypeFromId(objectId)) {
        case EObjectType::Chunk: {
            return CreateSchemedChunkReader(
                // TODO(babenko): make configuable
                New<TChunkReaderConfig>(),
                dataSplit,
                std::move(masterChannel),
                std::move(nodeDirectory),
                std::move(blockCache));
        }

        default:
            THROW_ERROR_EXCEPTION("Unsupported data split type %s", 
                ~FormatEnum(TypeFromId(objectId)).Quote());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

