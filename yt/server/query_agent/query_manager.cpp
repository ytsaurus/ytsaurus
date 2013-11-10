#include "query_manager.h"

#include "config.h"
#include "private.h"

#include <ytlib/chunk_client/block_cache.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/object_client/helpers.h>

#include <ytlib/new_table_client/config.h>
#include <ytlib/new_table_client/chunk_reader.h>
#include <ytlib/new_table_client/chunk_writer.h>
#include <ytlib/new_table_client/reader.h>

#include <ytlib/query_client/plan_fragment.h>

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
    : Config(std::move(config))
    , WorkerPool(New<TThreadPool>(Config->PoolSize, "QueryManager"))
    , Bootstrap(bootstrap)
{
    NodeDirectory = New<TNodeDirectory>();
    Evaluator = CreateEvaluator(WorkerPool->GetInvoker(), this);
}

TQueryManager::~TQueryManager()
{ }

void TQueryManager::UpdateNodeDirectory(
    const NNodeTrackerClient::NProto::TNodeDirectory& proto)
{
    NodeDirectory->MergeFrom(proto);
}

TAsyncError TQueryManager::Execute(
    const TPlanFragment& fragment,
    TWriterPtr writer)
{
    LOG_DEBUG("Executing plan fragment %s", ~ToString(fragment.Guid()));
    return Evaluator->Execute(fragment, std::move(writer));
}

IReaderPtr TQueryManager::GetReader(const TDataSplit& dataSplit)
{
    auto masterChannel = Bootstrap->GetMasterChannel();
    auto blockCache = Bootstrap->GetBlockStore()->GetBlockCache();

    auto objectId = FromProto<TObjectId>(dataSplit.chunk_id());
    LOG_DEBUG("Creating reader for %s", ~ToString(objectId));
    switch (TypeFromId(objectId)) {
    case EObjectType::Chunk: {
        return CreateChunkReader(
            New<TChunkReaderConfig>(),
            dataSplit,
            std::move(masterChannel),
            NodeDirectory,
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

