#pragma once

#include "public.h"

#include <core/concurrency/public.h>

#include <ytlib/query_client/callbacks.h>
#include <ytlib/query_client/executor.h>

#include <server/cell_node/public.h>

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

    virtual TAsyncError Execute(
        const NQueryClient::TPlanFragment& fragment,
        NQueryClient::ISchemedWriterPtr writer) override;

    virtual NQueryClient::ISchemedReaderPtr GetReader(
        const NQueryClient::TDataSplit& dataSplit,
        NQueryClient::TPlanContextPtr context) override;

private:
    TQueryAgentConfigPtr Config_;
    NCellNode::TBootstrap* Bootstrap_;

    NConcurrency::TThreadPoolPtr WorkerPool_;

    NQueryClient::IExecutorPtr Evaluator_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryAgent
} // namespace NYT

