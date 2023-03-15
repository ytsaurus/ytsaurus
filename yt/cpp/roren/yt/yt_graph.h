#pragma once

#include <yt/cpp/roren/interface/fwd.h>
#include <yt/cpp/roren/interface/private/fwd.h>

#include <mapreduce/yt/interface/fwd.h>

#include <memory>
#include <optional>
#include <vector>

namespace NRoren {

class TYtPipelineConfig;

////////////////////////////////////////////////////////////////////////////////

class TYtGraph
{
public:
    using TTableNodeId = int;
    using TOperationNodeId = int;

private:
    class TTableNode;

    class TOperationNode;
    class TMapOperationNode;
    class TMapReduceOperationNode;

public:
    TYtGraph(const TYtPipelineConfig& config);
    ~TYtGraph();
    void Optimize();

    std::vector<std::vector<TYtGraph::TOperationNodeId>> GetOperationLevels() const;
    NYT::IOperationPtr StartOperation(const NYT::IClientBasePtr& client, TOperationNodeId id) const;

    TString DumpDOTSubGraph(const TString& name) const;
    TString DumpDOT(const TString& prefix) const;  // dump graph in DOT format

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
        NPrivate::IRawTransformPtr rawTransform);

    TOperationNodeId AddOperationNode(std::unique_ptr<TOperationNode> operationNode);

    void RemoveTable(TTableNodeId tableId);
    void RemoveOperation(TOperationNodeId operationId);

private:
    const TYtPipelineConfig& Config_;
    std::vector<TTableNode> TableNodes_;
    std::vector<std::unique_ptr<TOperationNode>> OperationNodes_;

private:
    friend class TBuildingVisitor;
    friend std::unique_ptr<TYtGraph> BuildYtGraph(const TPipeline& pipeline, TString workingDir);
    friend void OptimizeYtGraph(TYtGraph* graph);
};

std::unique_ptr<TYtGraph> BuildYtGraph(const TPipeline& pipeline, const TYtPipelineConfig& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
