#pragma once

#include "fwd.h"

#include "bigrt.h"
#include "graph/parser.h"  //TODO: remove
#include "sync_execution_block_v3.h"
#include "async_execution_block_v3.h"
#include "parallel_execution_block_v3.h"

#include <yt/cpp/roren/interface/private/raw_transform.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/library/tvm/service/public.h>

#include <vector>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

enum class EParallelizationType
{
    ByGreedy,
    ByPart,
    ByKey,
    ByNone,
    Any
};

////////////////////////////////////////////////////////////////////////////////

struct TParsedGraphV3
{
    struct TRawGraphNode {
        int ExecutionBlockId = -1;
        TString InputTag;
        const TTransformNode* TransformNode = nullptr;
        EParallelizationType ParallelizationType = EParallelizationType::Any;
        std::vector<std::vector<TRawGraphNode*>> Outputs;
    };

    struct TRawExecutionBlock {
        int Id = -1;
        EParallelizationType ParallelizationType = EParallelizationType::Any;
        std::vector<TRawGraphNode*> Nodes;
        std::vector<std::vector<TRawExecutionBlock*>> Outputs;
    };

    std::list<TRawGraphNode> Nodes_;
    std::list<TRawExecutionBlock> ExecutionBlocks_;
    THashMap<TString, TRawExecutionBlock*> InputBlocks_;

    TString DotQuotify(TString s) const;
    TString DotEscQuotify(TString s) const;
    TString DotTransformId(const TRawGraphNode& node) const;
    TString DotTransformOutputId(const TRawGraphNode& node, size_t i) const;
    TString DotTransformLabel(const TRawGraphNode& node) const;
    TString DotTransformNode(const TRawGraphNode& node) const;
    TString DotTransformOutputLabel(const TRawGraphNode& node, size_t i) const;
    TString DotTransformOutput(const TRawGraphNode& node, size_t i) const;
    TString DotExecutionBlock(const TRawExecutionBlock& executionBlock) const;
    TString DumpDOT() const
    {
        TStringStream os;
        os << "digraph {" << Endl;
        for (const auto& graphNode: Nodes_) {
            os << DotTransformNode(graphNode);
        }
        for (const auto&executionBlock : ExecutionBlocks_) {
            os << DotExecutionBlock(executionBlock);
        }
        os << "}" << Endl;
        return os.Str();
    }

};

////////////////////////////////////////////////////////////////////////////////

class TGraphParserV3
    : private TParsedGraphV3
{
public:
    using TParsedGraphV3::TRawGraphNode;
    using TParsedGraphV3::TRawExecutionBlock;
    TParsedGraphV3 Parse(const TPipeline& pipeline, const bool validate);
    const std::vector<TWriterRegistrator>& GetWriterRegistrators() const noexcept;

protected:
    TRawGraphNode* AddInput(const TString& inputTag, const TTransformNode* transformNode);
    TRawGraphNode* AddNode(const TString& inputTag, const TTransformNode* transformNode, EParallelizationType parallelizationType);

protected:
    using TParsedGraphV3::Nodes_;
    using TParsedGraphV3::ExecutionBlocks_;
    using TParsedGraphV3::InputBlocks_;

    THashMap<TString, TRawGraphNode*> InputNodes_;
    THashMap<const TTransformNode*, TRawGraphNode*> NodesIndex_;
    THashMap<int, TRawExecutionBlock*> ExecutionBlocksIndex_;
    std::vector<TWriterRegistrator> WriterRegistrators_;
}; // TGraphParserV3

////////////////////////////////////////////////////////////////////////////////

class TSyncExecutionBlockBuilderV3
{
public:
    TSyncExecutionBlockBuilderV3(const TParsedGraphV3::TRawExecutionBlock& rawExecutionBlock);
    TSyncExecutionBlockV3Ptr Build();

private:
    IRawParDoPtr AddTransfomNode(const TTransformNode& transformNode);
    IRawParDoPtr AddParDo(const IRawParDo& transform);
    IRawParDoPtr AddStatefulParDo(const IRawStatefulParDo& transform, const TRawPStateNode& pStateNode);
    IRawParDoPtr AddStatefulTimerParDo(const IRawStatefulTimerParDo& transform, const TRawPStateNode& pStateNode);
    std::vector<TParDoTreeBuilder::TPCollectionNodeId> AddWrappedTransform(TParDoTreeBuilder::TPCollectionNodeId inputId, IRawParDoPtr transformPtr);
    TString AddStateManager(const TRawPStateNode& pStateNode);
    void AddTimer(const IRawStatefulTimerParDo& transform, TStatefulTimerParDoWrapperPtr wrappedTransform);

private:
    THashMap<TString, TCreateBaseStateManagerFunction> CreateBaseStateManagerFunctions_;
    THashMap<TString, TStatefulTimerParDoWrapperPtr> RegisteredTimers_;
    TParDoTreeBuilder ParDoTreeBuilder_;
};  // class TSyncExecutionBlockBuilderV3

////////////////////////////////////////////////////////////////////////////////

class TExecutionBlockFactoryV3
{
public:
    TExecutionBlockFactoryV3(const TParsedGraphV3::TRawExecutionBlock& rawExecutionBlock, NYT::TCancelableContextPtr cancelableContext);

    TSyncExecutionBlockV3Ptr BuildSyncExecutionBlock() const;
    TAsyncExecutionBlockV3Ptr BuildAsyncExecutionBlock(NYT::IInvokerPtr invoker, int fiberIndex) const;
    std::vector<TAsyncExecutionBlockV3Ptr> BuildManyAsyncExecutionBlocks(NYT::IInvokerPtr invoker, int fibers) const;
    IParallelExecutionBlockV3Ptr BuildParallelExecutionBlock(NYT::IInvokerPtr invoker, int fibers) const;

private:
    TParsedGraphV3::TRawExecutionBlock RawExecutionBlock_;
    NYT::TCancelableContextPtr CancelableContext_;
};

////////////////////////////////////////////////////////////////////////////////

class TParsedPipelineV3
{
public:
    TParsedPipelineV3(const TPipeline& pipeline, const bool validate);
    IParallelExecutionBlockV3Ptr GetPrepareDataExecutionBlock(NYT::IInvokerPtr invoker, int fibers, const TString& inputTag,  NYT::TCancelableContextPtr cancelableContext) const;
    IParallelExecutionBlockV3Ptr GetProcessPreparedDataExecutionBlock(NYT::IInvokerPtr invoker, int fibers, const TString& inputTag,  NYT::TCancelableContextPtr cancelableContext) const;

    TString GetDebugDescription() const noexcept;
    const std::vector<TWriterRegistrator>& GetWriterRegistrators() const noexcept;

private:
    TParsedGraphV3 ParsedGraph_;
    std::vector<TWriterRegistrator> WriterRegistrators_;
}; // class ParsedPipelineV3

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
