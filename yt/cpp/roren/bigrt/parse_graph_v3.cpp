#include "parse_graph_v3.h"

#include "execution_block.h"
#include "graph/parser.h" // InputTag.
#include "execution_graph_v3.h"

#include <yt/cpp/roren/interface/roren.h>
#include <yt/cpp/roren/library/logger/logger.h>

#include <util/generic/guid.h>
#include <util/string/join.h>

USE_ROREN_LOGGER();

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

static const TString DotEndl("<endl>");

bool IsPureTransform(const IRawTransform& rawTransform)
{
    const auto type = rawTransform.GetType();
    switch (type) {
        case ERawTransformType::Read:
            return true;
        case ERawTransformType::Write:
        case ERawTransformType::StatefulParDo:
        case ERawTransformType::StatefulTimerParDo:
        case ERawTransformType::Flatten:
            return false;
        case ERawTransformType::ParDo:
            return TFnAttributesOps::GetIsPure(rawTransform.AsRawParDoRef().GetFnAttributes());
        case ERawTransformType::GroupByKey:
        case ERawTransformType::CombinePerKey:
        case ERawTransformType::CombineGlobally:
        case ERawTransformType::CoGroupByKey:
            ythrow yexception() << "Unexpected transform type: " << type;
    }
    Y_ABORT("Unreachable branch");
}

bool isPureTransform(const TTransformNode& transformNode)
{
    if (transformNode.GetSinkCount() != 1) {
        return false;
    }
    return IsPureTransform(*transformNode.GetRawTransform());
}

EParallelizationType GetParallelizationType(const TTransformNode& transformNode)
{
    const auto& rawTransform = *transformNode.GetRawTransform();
    const auto type = rawTransform.GetType();
    switch (type) {
        case ERawTransformType::Read:
            return EParallelizationType::ByGreedy;
        case ERawTransformType::ParDo: {
            if (NPrivate::GetAttribute(rawTransform, WriterRegistratorTag)) {
                return EParallelizationType::ByNone;
            }
            return isPureTransform(transformNode)? EParallelizationType::Any: EParallelizationType::ByPart;
        }
        case ERawTransformType::Write:
            return EParallelizationType::ByNone;
        case ERawTransformType::StatefulParDo:
        case ERawTransformType::StatefulTimerParDo:
            return EParallelizationType::ByKey;
        case ERawTransformType::Flatten:
            return EParallelizationType::Any; //TODO: remove
        case ERawTransformType::GroupByKey:
        case ERawTransformType::CombinePerKey:
        case ERawTransformType::CombineGlobally:
        case ERawTransformType::CoGroupByKey:
            ythrow yexception() << "Unexpected transform type: " << type;
    }
}

////////////////////////////////////////////////////////////////////////////////

TString TParsedGraphV3::DotQuotify(TString s) const
{
    return TString("\"") + s + "\"";
}

TString TParsedGraphV3::DotEscQuotify(TString s) const
{
    return TString("\\\"") + s + "\\\"";
}

TString TParsedGraphV3::DotTransformId(const TRawGraphNode& node) const
{
    TStringStream result;
    result << Hex(reinterpret_cast<size_t>(node.TransformNode));
    return result.Str();
}

TString TParsedGraphV3::DotTransformOutputId(const TRawGraphNode& node, size_t i) const
{
    TStringStream result;
    result << DotTransformId(node) << '_' << i;
    return result.Str();
}

TString TParsedGraphV3::DotTransformLabel(const TRawGraphNode& node) const
{
    TStringStream label;
    //const TString name = NPrivate::GetAttributeOrDefault(*node.TransformNode->GetRawTransform(), TransformNameTag, {"<unknown>"});
    label << "inputTag=" << node.InputTag;
    label << "\\n";
    label << "name=" << node.TransformNode->GetName();
    label << "\\n";
    label << "type=" << node.TransformNode->GetRawTransform()->GetType();
    label << "\\n";
    label << "parallelization=" << node.ParallelizationType;
    label << "\\n";
    label << "ExecutionBlockId=" << node.ExecutionBlockId;
    //yexception() << node.TransformNode->GetRawTransform()->GetType();
    return label.Str();
}

TString TParsedGraphV3::DotTransformNode(const TRawGraphNode& node) const
{
    TStringStream result;
    result << DotQuotify(DotTransformId(node)) << " [shape=\"square\", label=" << DotQuotify(DotTransformLabel(node)) << "];" << DotEndl;
    for (size_t i = 0; i < std::size(node.Outputs); ++i) {
        result << DotTransformOutput(node, i);
    }
    return result.Str();
}

TString TParsedGraphV3::DotTransformOutputLabel(const TRawGraphNode& node, size_t i) const
{
    Y_UNUSED(node);
    return ToString(i);
}

TString TParsedGraphV3::DotTransformOutput(const TRawGraphNode& node, size_t i) const
{
    TStringStream result;
    result << DotQuotify(DotTransformOutputId(node, i)) << " [shape=\"circle\", label=" << DotQuotify(DotTransformOutputLabel(node, i)) << "];" << DotEndl;
    result << DotQuotify(DotTransformId(node)) << "->" << DotQuotify(DotTransformOutputId(node, i)) << ";" << DotEndl;
    for (const TRawGraphNode* next : node.Outputs[i]) {
        result << DotQuotify(DotTransformOutputId(node, i)) << "->" << DotQuotify(DotTransformId(*next)) << ";" << DotEndl;
    }
    return result.Str();
}

TString TParsedGraphV3::DotExecutionBlock(const TRawExecutionBlock& executionBlock) const
{
    TStringStream result;
    result << "subgraph cluster_" << executionBlock.Id << "{" << DotEndl;
    result << "label=ExecutionBlock" << executionBlock.Id << ';' << DotEndl;
    std::vector<TString> nodeIds;
    for (const auto* node : executionBlock.Nodes) {
        nodeIds.push_back(DotQuotify(DotTransformId(*node)));
        for (size_t i = 0; i < std::size(node->Outputs); ++i) {
            nodeIds.push_back(DotQuotify(DotTransformOutputId(*node, i)));
        }
    }
    result << JoinSeq(", ", nodeIds) << ';' << DotEndl;
    result << "}" << DotEndl;
    return result.Str();
}

////////////////////////////////////////////////////////////////////////////////

TParsedGraphV3 TGraphParserV3::Parse(const TPipeline& pipeline, const bool validate)
{
    // Add all nodes to graph
    for (const TTransformNodePtr& transformNode : GetRawPipeline(pipeline)->GetTransformList()) {
        const auto& rawTransform = *transformNode->GetRawTransform();
        const auto type = rawTransform.GetType();
        if (type == ERawTransformType::Read) {
            if (GetAttributeOrDefault(rawTransform, NRoren::NPrivate::BindToDictTag, false)) {
                continue;
            }
            const TString& inputTag = GetRequiredAttribute(rawTransform, InputTag);
            auto root = AddInput(inputTag, transformNode.Get());
            Y_UNUSED(root);
        }
        if (const auto* registrator = NPrivate::GetAttribute(rawTransform, WriterRegistratorTag)) {
            WriterRegistrators_.push_back(*registrator);
        }
    }

    // BuildExecutionBlocks
    TSet<TRawGraphNode*> inputGraphNodes;
    for (auto& [inputTag, node] : InputNodes_) {
        inputGraphNodes.insert(node);
    }
    TSet<TRawGraphNode*> nextExecutionBlocks = inputGraphNodes;
    while (!nextExecutionBlocks.empty()) {
        const int executionBlockId = ExecutionBlocks_.size();
        ExecutionBlocks_.push_back({});
        auto& currentExecutionBlock = ExecutionBlocks_.back();
        currentExecutionBlock.Id = executionBlockId;
        TRawGraphNode* currentGraphNode = *nextExecutionBlocks.begin();
        nextExecutionBlocks.erase(nextExecutionBlocks.begin());
        TSet<TRawGraphNode*> currentExecutionBlockNodes;
        ExecutionBlocksIndex_[currentExecutionBlock.Id] = &currentExecutionBlock;
        if (inputGraphNodes.contains(currentGraphNode)) {
            InputBlocks_[currentGraphNode->InputTag] = &currentExecutionBlock;
        }
        currentExecutionBlockNodes.insert(currentGraphNode);
        currentExecutionBlock.ParallelizationType = currentGraphNode->ParallelizationType;
        while (!currentExecutionBlockNodes.empty()) {
            TRawGraphNode* node = *currentExecutionBlockNodes.begin();
            currentExecutionBlockNodes.erase(currentExecutionBlockNodes.begin());
            node->ExecutionBlockId = executionBlockId;
            currentExecutionBlock.Nodes.push_back(node);
            for (auto& output : node->Outputs) {
                for (auto& nextNode : output) {
                    if (currentExecutionBlock.ParallelizationType == nextNode->ParallelizationType) {
                        currentExecutionBlockNodes.insert(nextNode);
                    } else {
                        nextExecutionBlocks.insert(nextNode);
                    }
                }
            }
        }
        if (validate) {
            YT_LOG_FATAL_IF(currentExecutionBlock.ParallelizationType == EParallelizationType::ByGreedy && currentExecutionBlock.Nodes.size() == 1, "Input '%v' hasn't prepare part", currentGraphNode->InputTag);
        }
    }

    // calculate next graphNodes
    for (auto& executionBlock : ExecutionBlocks_) {
        for (const auto* graphNode : executionBlock.Nodes) {
            for (const auto& output : graphNode->Outputs) {
                std::vector<TRawExecutionBlock*> nextExecutionBlocks;
                for (const auto* nextNode : output) {
                    if (nextNode->ExecutionBlockId != executionBlock.Id) {
                        auto* nextExecutionBlock = ExecutionBlocksIndex_.at(nextNode->ExecutionBlockId);
                        nextExecutionBlocks.push_back(nextExecutionBlock);
                    }
                }
                if (!nextExecutionBlocks.empty()) {
                    executionBlock.Outputs.push_back(std::move(nextExecutionBlocks));
                }
            }
        }
    }
    return std::move(*this);
}

TGraphParserV3::TRawGraphNode* TGraphParserV3::AddInput(const TString& inputTag, const TTransformNode* transformNode)
{
    auto* node = AddNode(inputTag, transformNode, EParallelizationType::ByGreedy);
    InputNodes_[inputTag] = node;
    return node;
}

TGraphParserV3::TRawGraphNode* TGraphParserV3::AddNode(const TString& inputTag, const TTransformNode* transformNode, EParallelizationType parallelizationType)
{
    if (NodesIndex_.contains(transformNode)) {
        TRawGraphNode* const result = NodesIndex_[transformNode];
        Y_ABORT_IF(result->InputTag != inputTag);
        return result;
    }
    Nodes_.push_back({});
    TRawGraphNode& result = Nodes_.back();
    NodesIndex_[transformNode] = &result;
    result.InputTag = inputTag;
    result.TransformNode = transformNode;
    result.ParallelizationType = GetParallelizationType(*result.TransformNode);
    if (result.ParallelizationType == EParallelizationType::Any) {
        result.ParallelizationType = parallelizationType;
    }
    for (const auto& sink : result.TransformNode->GetSinkList()) {
        result.Outputs.push_back({});
        for (const auto& nextTransformNode : sink->GetSourceFor()) {
            result.Outputs.back().push_back(AddNode(inputTag, nextTransformNode, result.ParallelizationType));
        }
    }
    return &result;
}

const std::vector<TWriterRegistrator>& TGraphParserV3::GetWriterRegistrators() const noexcept
{
    return WriterRegistrators_;
}

////////////////////////////////////////////////////////////////////////////////

TSyncExecutionBlockBuilderV3::TSyncExecutionBlockBuilderV3(const TGraphParserV3::TRawExecutionBlock& rawExecutionBlock)
{
    Y_ABORT_IF(rawExecutionBlock.Nodes.empty());
    TSet<const TTransformNode*> ownTransformNodes;
    for (const TGraphParserV3::TRawGraphNode* rawGraphNode : rawExecutionBlock.Nodes) {
        ownTransformNodes.insert(rawGraphNode->TransformNode);
    }
    std::deque<std::pair<TParDoTreeBuilder::TPCollectionNodeId, const TTransformNode*>> queue;
    queue.push_back({TParDoTreeBuilder::RootNodeId, rawExecutionBlock.Nodes.front()->TransformNode});
    while (!queue.empty()) {
        auto [inputId, transformNode] = queue.front();
        queue.pop_front();
        auto wrappedTransformPtr = AddTransfomNode(*transformNode);
        if (!wrappedTransformPtr) {
            // Skip this transform and add next transforms.
            const auto sinksCount = transformNode->GetSinkCount();
            Y_ABORT_IF(sinksCount > 1);
            if (sinksCount > 0) {
                auto nextTransforms = transformNode->GetSink(0)->GetSourceFor();
                for (const TTransformNode* nextTransform : nextTransforms) {
                    if (ownTransformNodes.contains(nextTransform)) {
                        queue.emplace_back(inputId, nextTransform);
                    } else {
                        ParDoTreeBuilder_.MarkAsOutput(inputId);
                    }
                }
            }
            continue;
        }
        auto outputNodeIds = AddWrappedTransform(inputId, wrappedTransformPtr);
        const auto& sinkNodeList = transformNode->GetSinkList();
        Y_ABORT_IF(std::ssize(outputNodeIds) != std::ssize(sinkNodeList));
        for (ssize_t i = 0; i < std::ssize(sinkNodeList); ++i) {
            auto nextTransforms = sinkNodeList[i]->GetSourceFor();
            for (const TTransformNode* nextTransform : nextTransforms) {
                if (ownTransformNodes.contains(nextTransform)) {
                    queue.emplace_back(outputNodeIds[i], nextTransform);
                } else {
                    ParDoTreeBuilder_.MarkAsOutput(outputNodeIds[i]);
                }
            }
        }
    }
}

TSyncExecutionBlockV3Ptr TSyncExecutionBlockBuilderV3::Build()
{
    return MakeIntrusive<TSyncExecutionBlockV3>(ParDoTreeBuilder_.Build(), std::move(CreateBaseStateManagerFunctions_), std::move(RegisteredTimers_));
}

IRawParDoPtr TSyncExecutionBlockBuilderV3::AddTransfomNode(const TTransformNode& transformNode)
{
    const IRawTransform& transform = *transformNode.GetRawTransform();
    auto type = transform.GetType();
    switch (type)
    {
        case ERawTransformType::Read:
            return {};
        case ERawTransformType::ParDo:
            return AddParDo(transform.AsRawParDoRef());
        case ERawTransformType::StatefulParDo:
            return AddStatefulParDo(transform.AsRawStatefulParDoRef(), *transformNode.GetPStateNode());
        case ERawTransformType::StatefulTimerParDo:
            return AddStatefulTimerParDo(transform.AsRawStatefulTimerParDoRef(), *transformNode.GetPStateNode());
        case ERawTransformType::Flatten:
            return {};
        case ERawTransformType::Write:
            //return AddParDo(transform.AsRawWriteRef());
        default:
            ythrow yexception() << "Unexpected transform type: " << type;
    }
}

std::vector<TParDoTreeBuilder::TPCollectionNodeId> TSyncExecutionBlockBuilderV3::AddWrappedTransform(TParDoTreeBuilder::TPCollectionNodeId inputId, IRawParDoPtr transform)
{
    return ParDoTreeBuilder_.AddParDo(std::move(transform), inputId);
}

IRawParDoPtr TSyncExecutionBlockBuilderV3::AddParDo(const IRawParDo& transform)
{
    return transform.Clone();
}

IRawParDoPtr TSyncExecutionBlockBuilderV3::AddStatefulParDo(const IRawStatefulParDo& transform, const TRawPStateNode& pStateNode)
{
    const auto stateVtable = NPrivate::GetRequiredAttribute(pStateNode, BigRtStateManagerVtableTag);
    const auto stateConfig = NPrivate::GetRequiredAttribute(pStateNode, BigRtStateConfigTag);
    auto wrappedTransform = CreateStatefulParDo(transform.Clone(), AddStateManager(pStateNode), stateVtable, stateConfig);
    return wrappedTransform;
}

IRawParDoPtr TSyncExecutionBlockBuilderV3::AddStatefulTimerParDo(const IRawStatefulTimerParDo& transform, const TRawPStateNode& pStateNode)
{
    const auto stateVtable = NPrivate::GetRequiredAttribute(pStateNode, BigRtStateManagerVtableTag);
    const auto stateConfig = NPrivate::GetRequiredAttribute(pStateNode, BigRtStateConfigTag);
    auto wrappedTransform = CreateStatefulTimerParDo(transform.Clone(), AddStateManager(pStateNode), stateVtable, stateConfig);
    AddTimer(transform, wrappedTransform);
    return wrappedTransform;
}

void TSyncExecutionBlockBuilderV3::AddTimer(const IRawStatefulTimerParDo& transform, TStatefulTimerParDoWrapperPtr wrappedTransform)
{
    RegisteredTimers_[transform.GetFnId()] = wrappedTransform;
}

TString TSyncExecutionBlockBuilderV3::AddStateManager(const TRawPStateNode& pStateNode)
{
    TString id = TString{"state-manager-"} + TGUID::Create().AsGuidString();
    const auto createStateManagerFunction = NPrivate::GetRequiredAttribute(pStateNode, NPrivate::CreateBaseStateManagerFunctionTag);
    CreateBaseStateManagerFunctions_[id] = createStateManagerFunction;
    return id;
}

////////////////////////////////////////////////////////////////////////////////

TExecutionBlockFactoryV3::TExecutionBlockFactoryV3(const TGraphParserV3::TRawExecutionBlock& rawExecutionBlock, NYT::TCancelableContextPtr cancelableContext)
    : RawExecutionBlock_(rawExecutionBlock)
    , CancelableContext_(std::move(cancelableContext))
{
}

TSyncExecutionBlockV3Ptr TExecutionBlockFactoryV3::BuildSyncExecutionBlock() const
{
    TSyncExecutionBlockBuilderV3 builder(RawExecutionBlock_);
    return builder.Build();
}

TAsyncExecutionBlockV3Ptr TExecutionBlockFactoryV3::BuildAsyncExecutionBlock(NYT::IInvokerPtr invoker, int fiberIndex) const
{
    return MakeIntrusive<TAsyncExecutionBlockV3>(fiberIndex, std::move(invoker), BuildSyncExecutionBlock(), CancelableContext_);
}

std::vector<TAsyncExecutionBlockV3Ptr> TExecutionBlockFactoryV3::BuildManyAsyncExecutionBlocks(NYT::IInvokerPtr invoker, int fibers) const
{
    std::vector<TAsyncExecutionBlockV3Ptr> asyncExecutionBlocks;
    asyncExecutionBlocks.reserve(fibers);
    for (int i = 0; i < fibers; ++i) {
        asyncExecutionBlocks.emplace_back(BuildAsyncExecutionBlock(invoker, i));
    }
    return asyncExecutionBlocks;
}

IParallelExecutionBlockV3Ptr TExecutionBlockFactoryV3::BuildParallelExecutionBlock(NYT::IInvokerPtr invoker, int fibers) const
{
    switch (RawExecutionBlock_.ParallelizationType) {
        case EParallelizationType::ByNone:
            return MakeParallelByNoneExecutionBlockV3(invoker, BuildManyAsyncExecutionBlocks(invoker, 1));
        case EParallelizationType::ByGreedy:
            return MakeParallelByGreedyExecutionBlockV3(invoker, BuildManyAsyncExecutionBlocks(invoker, fibers));
        case EParallelizationType::ByPart:
            return MakeParallelByPartExecutionBlockV3(invoker, BuildManyAsyncExecutionBlocks(invoker, fibers));
        case EParallelizationType::ByKey:
            return MakeParallelByKeyExecutionBlockV3(invoker, BuildManyAsyncExecutionBlocks(invoker, fibers));
        case EParallelizationType::Any:
            ythrow yexception() << "Unexpected parallelization type: " << RawExecutionBlock_.ParallelizationType;
    };
}

////////////////////////////////////////////////////////////////////////////////

TParsedPipelineV3::TParsedPipelineV3(const TPipeline& pipeline, const bool validate)
{
    TGraphParserV3 graphParser;
    ParsedGraph_ = graphParser.Parse(pipeline, validate);
    WriterRegistrators_ = graphParser.GetWriterRegistrators();
}

IParallelExecutionBlockV3Ptr TParsedPipelineV3::GetPrepareDataExecutionBlock(NYT::IInvokerPtr invoker, int fibers, const TString& inputTag,  NYT::TCancelableContextPtr cancelableContext) const
{
    TParsedGraphV3::TRawExecutionBlock* rawExecutionBlock = ParsedGraph_.InputBlocks_.at(inputTag);
    Y_ABORT_IF(rawExecutionBlock->ParallelizationType != EParallelizationType::ByGreedy);  //check for IsPure transforms
    TExecutionBlockFactoryV3 factory(*rawExecutionBlock, std::move(cancelableContext));
    return factory.BuildParallelExecutionBlock(std::move(invoker), fibers);
}

IParallelExecutionBlockV3Ptr TParsedPipelineV3::GetProcessPreparedDataExecutionBlock(NYT::IInvokerPtr invoker, int fibers, const TString& inputTag,  NYT::TCancelableContextPtr cancelableContext) const
{
    TParsedGraphV3::TRawExecutionBlock* rawExecutionBlock = ParsedGraph_.InputBlocks_.at(inputTag)->Outputs.at(0).at(0);
    return MakeIntrusive<TExecutionGraphV3>(std::move(invoker), fibers, std::move(cancelableContext), *rawExecutionBlock);
}

TString TParsedPipelineV3::GetDebugDescription() const noexcept
{
    return ParsedGraph_.DumpDOT();
}

const std::vector<TWriterRegistrator>& TParsedPipelineV3::GetWriterRegistrators() const noexcept
{
    return WriterRegistrators_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
