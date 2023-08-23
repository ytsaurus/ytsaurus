#pragma once

#include "yt_graph.h"

#include <memory>
#include <set>

namespace NRoren {
    class TPipeline;
    class IYtGraph;
    class TYtPipelineConfig;
}

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TYtGraphV2
    : public IYtGraph
{
public:
    struct TTableNode;
    using TTableNodePtr = std::shared_ptr<TTableNode>;
    struct TOperationNode;
    using TOperationNodePtr = std::shared_ptr<TOperationNode>;

    class TPlainGraph;

public:
    explicit TYtGraphV2(std::unique_ptr<TPlainGraph> plainGraph);
    ~TYtGraphV2() override;

    void Optimize() override;

    std::vector<std::vector<TOperationNodeId>>
    GetOperationLevels() const override;

    NYT::IOperationPtr StartOperation(const NYT::IClientBasePtr &client,
                                      TOperationNodeId id) const override;

    TString DumpDOTSubGraph(const TString &name) const override;

    TString DumpDOT(const TString &prefix) const override;

    std::set<TString> GetEdgeDebugStringSet() const;

private:
    std::unique_ptr<TPlainGraph> PlainGraph_;

    friend class TYtGraphV2Builder;
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TYtGraphV2> BuildYtGraphV2(const TPipeline& pipeline, const TYtPipelineConfig& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
