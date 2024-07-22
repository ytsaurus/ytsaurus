#pragma once

#include <yt/cpp/mapreduce/interface/fwd.h>
#include <yt/cpp/roren/interface/fwd.h>
#include <yt/cpp/roren/interface/private/fwd.h>

#include <memory>
#include <vector>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TYtPipelineConfig;

////////////////////////////////////////////////////////////////////////////////

struct TStartOperationContext
{
    std::shared_ptr<const TYtPipelineConfig> Config;
};

class IYtGraph
{
public:
    using TTableNodeId = ssize_t;
    using TOperationNodeId = ssize_t;

public:
    virtual ~IYtGraph() = default;

    virtual void Optimize() = 0;

    virtual std::vector<std::vector<TOperationNodeId>> GetOperationLevels() const = 0;
    virtual NYT::IOperationPtr StartOperation(const NYT::IClientBasePtr& client, TOperationNodeId id, const TStartOperationContext& context) const = 0;

    virtual TString DumpDOTSubGraph(const TString& name) const = 0;
    virtual TString DumpDOT(const TString& prefix) const = 0;  // dump graph in DOT format
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
