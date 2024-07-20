#pragma once

#include "yt_graph.h"

#include <yt/cpp/roren/yt/proto/config.pb.h>

#include <yt/cpp/mapreduce/util/temp_table.h>

#include <memory>

namespace NRoren {

class TPipeline;
class IYtGraph;

} // namespace NRoren

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TYtGraphV2
    : public IYtGraph
{
public:
    class TTableNode;
    using TTableNodePtr = std::shared_ptr<TTableNode>;
    class TOperationNode;
    using TOperationNodePtr = std::shared_ptr<TOperationNode>;

    class TPlainGraph;

public:
    explicit TYtGraphV2(std::unique_ptr<TPlainGraph> plainGraph, const TYtPipelineConfig& config);
    ~TYtGraphV2() override;

    void Optimize() override;

    std::vector<std::vector<TOperationNodeId>>
    GetOperationLevels() const override;

    THashMap<TOperationNodeId, std::vector<TOperationNodeId>> GetNextOperationMapping() const;

    NYT::IOperationPtr StartOperation(
        const NYT::IClientBasePtr &client,
        TOperationNodeId id,
        const TStartOperationContext& context) const override;

    TString DumpDOTSubGraph(const TString &name) const override;

    TString DumpDOT(const TString &prefix) const override;

    void CreateWorkingDir(NYT::IClientBasePtr client) const;
    void ClearIntermediateTables() const;
    void LeaveIntermediateTables(NYT::IClientBasePtr client, NYT::ITransactionPtr tx, TOperationNodeId operationNodeId) const;

private:
    std::unique_ptr<TPlainGraph> PlainGraph_;
    TYtPipelineConfig Config_;
    mutable std::vector<std::pair<THolder<NYT::TTempTable>, TOperationNodeId>> IntermediateTables_;

    friend class TYtGraphV2Builder;
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TYtGraphV2> BuildYtGraphV2(const TPipeline& pipeline, const TYtPipelineConfig& config);
std::shared_ptr<TYtGraphV2> BuildOptimizedYtGraphV2(const TPipeline& pipeline, const TYtPipelineConfig& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
