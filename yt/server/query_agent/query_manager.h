#pragma once

#include "public.h"

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/executor.h>

#include <core/concurrency/action_queue.h>

#include <server/cell_node/public.h>

namespace NYT { namespace NNodeTrackerClient { namespace NProto {
    class TNodeDirectory;
} } }

namespace NYT {
namespace NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

class TQueryManager
    : public NQueryClient::IExecutor
    , public NQueryClient::IEvaluateCallbacks
{
public:
    TQueryManager(
        TQueryAgentConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    ~TQueryManager();

    void UpdateNodeDirectory(
        const NNodeTrackerClient::NProto::TNodeDirectory& proto);

    virtual TAsyncError Execute(
        const NQueryClient::TPlanFragment& fragment,
        NQueryClient::IWriterPtr writer) override;

    virtual NQueryClient::IReaderPtr GetReader(
        const NQueryClient::TDataSplit& dataSplit) override;

private:
    TQueryAgentConfigPtr Config;
    NConcurrency::TThreadPoolPtr WorkerPool;
    NCellNode::TBootstrap* Bootstrap;

    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory;
    NQueryClient::IExecutorPtr Evaluator;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NExecAgent
} // namespace NYT

