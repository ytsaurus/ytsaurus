#pragma once

#include <yt/cpp/roren/yt/interface_graph.h>
#include <yt/cpp/roren/yt/state.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TYtGraph
    : public IYtGraph
{
private:
    class TTableNode;

    class TOperationNode;
    class TMapOperationNode;
    class TMapReduceOperationNode;

public:
    TYtGraph(const TYtPipelineConfig& config);
    ~TYtGraph() override;
    void Optimize() override;

    std::vector<std::vector<TYtGraph::TOperationNodeId>> GetOperationLevels() const override;
    NYT::IOperationPtr StartOperation(const NYT::IClientBasePtr& client, TOperationNodeId id, const TStartOperationContext& context) const override;

    TString DumpDOTSubGraph(const TString& name) const override;
    TString DumpDOT(const TString& prefix) const override;  // dump graph in DOT format

private:
    TTableNodeId AddTableNode(NYT::TRichYPath path, std::optional<NYT::TTableSchema> schema, NPrivate::TRowVtable rowVtable);

    TOperationNodeId AddMapOperationNode(
        TTableNodeId input,
        std::vector<TTableNodeId> outputs,
        NPrivate::IRawTransformPtr rawTransform);

    TOperationNodeId AddMapReduceOperationNode(
        std::vector<TTableNodeId> inputs,
        std::vector<TTableNodeId> mapOutputs,
        std::vector<TTableNodeId> outputs,
        NPrivate::IRawTransformPtr rawTransform,
        NPrivate::TYtStateVtable stateVtable);

    TOperationNodeId AddOperationNode(std::unique_ptr<TOperationNode> operationNode);

    void RemoveTable(TTableNodeId tableId);
    void RemoveOperation(TOperationNodeId operationId);

private:
    const TYtPipelineConfig& Config_;
    std::vector<TTableNode> TableNodes_;
    std::vector<std::unique_ptr<TOperationNode>> OperationNodes_;

private:
    friend class TBuildingVisitor;
    friend std::shared_ptr<TYtGraph> BuildYtGraph(const TPipeline& pipeline, TString workingDir);
    friend void OptimizeYtGraph(TYtGraph* graph);
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<IYtGraph> BuildYtGraph(const TPipeline& pipeline, const TYtPipelineConfig& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
