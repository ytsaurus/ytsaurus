#include "query_manager.h"

#include "config.h"
#include "private.h"

#include <ytlib/chunk_client/block_cache.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/public.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/chunk_reader.h>
#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/reader.h>

#include <ytlib/query_client/query_fragment.h>

#include <server/data_node/block_store.h>

#include <server/cell_node/bootstrap.h>

namespace NYT {
namespace NQueryAgent {

using namespace NCellNode;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NQueryClient;
using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = QueryAgentLogger;
static auto& Profiler = QueryAgentProfiler;
static auto ProfilingPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

TQueryManager::TQueryManager(
    TQueryAgentConfigPtr config,
    TBootstrap* bootstrap)
    : Config(std::move(config))
    , WorkerPool(New<TThreadPool>(Config->PoolSize, "QueryManager"))
    , Bootstrap(bootstrap)
{
    Evaluator = CreateEvaluator(WorkerPool->GetInvoker(), this);
}

TQueryManager::~TQueryManager()
{ }

TAsyncError TQueryManager::Execute(
    const TQueryFragment& fragment,
    TWriterPtr writer)
{
    LOG_DEBUG("Executing query fragment %s", ~ToString(fragment.Guid()));
    return Evaluator->Execute(fragment, std::move(writer));
}

IReaderPtr TQueryManager::GetReader(const TDataSplit& dataSplit)
{
    TDataSplit dataSplitCopy = dataSplit;
    dataSplitCopy.clear_replicas();
    auto masterChannel = Bootstrap->GetMasterChannel();
    auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    auto blockCache = Bootstrap->GetBlockStore()->GetBlockCache();

    auto objectId = FromProto<TObjectId>(dataSplit.chunk_id());
    LOG_DEBUG("Creating reader for %s", ~ToString(objectId));
    switch (TypeFromId(objectId)) {
    case EObjectType::Chunk: {
        return CreateChunkReader(
            New<TChunkReaderConfig>(),
            dataSplitCopy,
            std::move(masterChannel),
            std::move(nodeDirectory),
            std::move(blockCache));
    }
    default:
        THROW_ERROR_EXCEPTION(
            "Unsupported data split type \"%s\"", 
            ~TypeFromId(objectId).ToString());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

