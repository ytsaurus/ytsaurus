#include "par_do_tree.h"

#include "fn_attributes_ops.h"

#include "../fns.h"

#include <yt/cpp/roren/interface/execution_context.h>
#include <yt/cpp/roren/interface/roren.h>

#include <yt/yt/core/ytree/convert.h>

#include <util/generic/iterator_range.h>
#include <util/generic/overloaded.h>

#include <util/string/builder.h>
#include <util/generic/hash_set.h>

#include <map>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPipePCollectionNode
    : public IRawOutput
{
public:
    TPipePCollectionNode(std::vector<IRawOutputPtr> outputs, std::vector<IRawParDoPtr> parDos)
        : Outputs_(std::move(outputs))
    {
        for (auto& parDo : parDos) {
            if (TFnAttributesOps::GetIsMove(parDo->GetFnAttributes())) {
                MoveParDos_.push_back(std::move(parDo));
            } else {
                CRefParDos_.push_back(std::move(parDo));
            }
        }
    }

    void AddRaw(const void* rows, ssize_t count) override
    {
        for (const auto& parDo : CRefParDos_) {
            parDo->Do(rows, count);
        }
        for (const auto& parDo : MoveParDos_) {
            parDo->Do(rows, count);
        }
        for (const auto& output : Outputs_) {
            output->AddRaw(rows, count);
        }
    }

    void MoveRaw(void* rows, ssize_t count) override
    {
        for (const auto& parDo : CRefParDos_) {
            parDo->Do(rows, count);
        }
        auto lastIdx = std::ssize(MoveParDos_) + std::ssize(Outputs_) - 1;
        for (ssize_t idx = 0; idx < std::ssize(Outputs_); ++idx) {
            if (Y_UNLIKELY(idx != lastIdx)) {
                Outputs_[idx]->AddRaw(rows, count);
            } else {
                Outputs_[idx]->MoveRaw(rows, count);
            }
        }
        lastIdx -= std::ssize(Outputs_);
        for (ssize_t idx = 0; idx < std::ssize(MoveParDos_); ++idx) {
            if (Y_UNLIKELY(idx != lastIdx)) {
                MoveParDos_[idx]->Do(rows, count);
            } else {
                MoveParDos_[idx]->MoveDo(rows, count);
            }
        }
    }

    void Close() override
    { }

private:
    std::vector<IRawOutputPtr> Outputs_;
    std::vector<IRawParDoPtr> CRefParDos_;
    std::vector<IRawParDoPtr> MoveParDos_;
};

using TPipePCollectionNodePtr = NYT::TIntrusivePtr<TPipePCollectionNode>;

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TParDoTreeBuilder::TParDoTree
    : public IParDoTree
{
public:
    TParDoTree() = default;

    TParDoTree(
        std::vector<TParDoNode> parDoNodes,
        const std::vector<TPCollectionNode>& pCollectionNodes,
        std::vector<TDynamicTypeTag> outputTags)
        : ParDoNodes_(std::move(parDoNodes))
        , InputVtable_(pCollectionNodes[0].RowVtable)
        , OutputTags_(std::move(outputTags))
        , FnAttributes_(MergeFnAttributes(ParDoNodes_))
    {
        for (const auto& node : pCollectionNodes) {
            PCollectionNodeOutputIndex_.push_back(node.GlobalOutputIndex);
        }
        InitializeOutputTags();
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag{"par-do-tree-input", InputVtable_}};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return OutputTags_;
    }

    std::vector<TDynamicTypeTag> GetOriginalOutputTags() const override
    {
        return OriginalOutputTags_;
    }

    const TFnAttributes& GetFnAttributes() const override
    {
        return FnAttributes_;
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& globalOutputs) override
    {
        InitializePipePCollectionNodes(globalOutputs);
        StartParDos(context);
    }

    void Do(const void* rows, int count) override
    {
        PipePCollectionNodes_[RootNodeId]->AddRaw(rows, count);
    }

    void MoveDo(void* rows, int count) override
    {
        PipePCollectionNodes_[RootNodeId]->MoveRaw(rows, count);
    }

    void Finish() override
    {
        for (const auto& parDoNode : ParDoTopoOrder()) {
            parDoNode.ParDo->Finish();
        }
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return NYT::New<TParDoTree>();
        };
    }

    void Save(IOutputStream* stream) const override
    {
        ::Save(stream, ParDoNodes_);
        ::Save(stream, PCollectionNodeOutputIndex_);
        ::Save(stream, InputVtable_);
        ::Save(stream, OutputTags_);
        ::Save(stream, FnAttributes_);
    }

    void Load(IInputStream* stream) override
    {
        ::Load(stream, ParDoNodes_);
        ::Load(stream, PCollectionNodeOutputIndex_);
        ::Load(stream, InputVtable_);
        ::Load(stream, OutputTags_);
        ::Load(stream, FnAttributes_);
        InitializeOutputTags();
    }

    TString GetDebugDescription() const override
    {
        NYT::TStringBuilder builder;
        builder.AppendString("TParDoTree=[\n");
        for (const auto& node : ParDoNodes_) {
            const auto name = NPrivate::GetAttributeOrDefault(*node.ParDo, TransformNameTag, TString{"<unknown>"});
            builder.AppendFormat(
                "    {name=%Qv; inputs=%v; outputs=%v};\n",
                name,
                NYT::NYson::ConvertToYsonString(node.Inputs, NYT::NYson::EYsonFormat::Text),
                NYT::NYson::ConvertToYsonString(node.Outputs, NYT::NYson::EYsonFormat::Text));
        }
        builder.AppendString("]\n");
        return builder.Flush();
    }

    void PrintDebugDescription() const
    {
        Cerr << GetDebugDescription() << Endl;
    }

private:
    TIteratorRange<typename std::vector<TParDoNode>::const_iterator>
    ParDoTopoOrder() {
        return TIteratorRange{ParDoNodes_.cbegin(), ParDoNodes_.cend()};
    }

    TIteratorRange<typename std::vector<TParDoNode>::const_reverse_iterator>
    ReverseParDoTopoOrder() {
        return TIteratorRange{ParDoNodes_.crbegin(), ParDoNodes_.crend()};
    }

    void InitializeOutputTags()
    {
        OriginalOutputTags_.resize(OutputTags_.size());

        auto getDescription = [] (int index) -> TString {
            return ::TStringBuilder() << "par-do-tree-" << index;
        };

        for (const auto& parDoNode : ParDoNodes_) {
            for (int localOutputIndex = 0; localOutputIndex < std::ssize(parDoNode.Outputs); ++localOutputIndex) {
                auto outputNodeId = parDoNode.Outputs[localOutputIndex];
                auto globalOutputIndex = PCollectionNodeOutputIndex_[outputNodeId];
                if (globalOutputIndex != InvalidOutputIndex) {
                    auto originalTag = parDoNode.ParDo->GetOutputTags()[localOutputIndex];
                    Y_ABORT_UNLESS(0 <= globalOutputIndex && globalOutputIndex < std::ssize(OutputTags_));
                    OriginalOutputTags_[globalOutputIndex] = originalTag;
                    if (!OutputTags_[globalOutputIndex]) {
                        OutputTags_[globalOutputIndex] = TDynamicTypeTag(getDescription(globalOutputIndex), originalTag.GetRowVtable());
                    }
                }
            }
        }
    }

    void InitializePipePCollectionNodes(const std::vector<IRawOutputPtr>& globalOutputs)
    {
        std::vector<std::pair<std::vector<IRawOutputPtr>, std::vector<IRawParDoPtr>>> pipePCollectionNodeOutputs(
            PCollectionNodeOutputIndex_.size());

        for (const auto& parDoNode : ParDoNodes_) {
            for (const auto input : parDoNode.Inputs) {
                pipePCollectionNodeOutputs[input].second.push_back(parDoNode.ParDo);
            }
        }

        for (int pCollectionIndex = 0; pCollectionIndex < std::ssize(PCollectionNodeOutputIndex_); ++pCollectionIndex) {
            auto globalOutputIndex = PCollectionNodeOutputIndex_[pCollectionIndex];
            if (globalOutputIndex != InvalidOutputIndex) {
                auto& pCollectionNodeOutputs = pipePCollectionNodeOutputs.at(pCollectionIndex);
                pCollectionNodeOutputs.first.emplace_back(globalOutputs.at(globalOutputIndex));
            }
        }

        PipePCollectionNodes_.clear();
        PipePCollectionNodes_.reserve(pipePCollectionNodeOutputs.size());
        for (auto& [outputs, parDos] : pipePCollectionNodeOutputs) {
            auto node = NYT::New<TPipePCollectionNode>(std::move(outputs), std::move(parDos));
            PipePCollectionNodes_.push_back(std::move(node));
        }
    }

    void StartParDos(const IExecutionContextPtr& context)
    {
        Y_ABORT_UNLESS(PipePCollectionNodes_.size() == PCollectionNodeOutputIndex_.size());

        std::vector<IRawOutputPtr> parDoOutputs;
        for (const auto& parDoNode : ReverseParDoTopoOrder()) {
            parDoOutputs.clear();
            parDoOutputs.reserve(parDoNode.Outputs.size());
            for (int pCollectionIndex : parDoNode.Outputs) {
                parDoOutputs.push_back(PipePCollectionNodes_[pCollectionIndex]);
            }
            parDoNode.ParDo->Start(context, parDoOutputs);
        }
    }

    static TFnAttributes MergeFnAttributes(const std::vector<TParDoNode>& parDoNodeList)
    {
        TFnAttributes result;
        result.SetIsPure(true); // Empty tree is pure.
        for (const auto& node : parDoNodeList) {
            const auto& currentAttributes = node.ParDo->GetFnAttributes();
            TFnAttributesOps::Merge(result, currentAttributes);
        }
        TFnAttributesOps::SetIsMove(result, TFnAttributesOps::GetIsMove(parDoNodeList[RootNodeId].ParDo->GetFnAttributes()));
        return result;
    }

private:
    // Serialized fields.
    std::vector<TParDoNode> ParDoNodes_;
    std::vector<int> PCollectionNodeOutputIndex_;
    TRowVtable InputVtable_;
    std::vector<TDynamicTypeTag> OutputTags_;
    TFnAttributes FnAttributes_;

    // Non-serialized fields.
    std::vector<TDynamicTypeTag> OriginalOutputTags_;
    std::vector<TPipePCollectionNodePtr> PipePCollectionNodes_;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TParDoTreeBuilder::TPCollectionNodeId> TParDoTreeBuilder::AddParDo(
    IRawParDoPtr parDo,
    TPCollectionNodeId input)
{
    return AddParDo(std::move(parDo), std::vector{input});
}

std::vector<TParDoTreeBuilder::TPCollectionNodeId> TParDoTreeBuilder::AddParDo(
    IRawParDoPtr parDo,
    std::vector<TPCollectionNodeId> inputs)
{
    for (const auto input : inputs) {
        Y_ABORT_UNLESS(0 <= input && input < std::ssize(PCollectionNodes_));
        const auto& tags = parDo->GetInputTags();
        Y_ABORT_UNLESS(tags.size() == 1);
        if (input == 0) {
            Y_ABORT_UNLESS(tags.size() == 1);
            if (!IsDefined(PCollectionNodes_[0].RowVtable)) {
                Y_ABORT_UNLESS(inputs.size() == 1);
                PCollectionNodes_[0].RowVtable = tags[0].GetRowVtable();
            }
        }
        Y_ABORT_UNLESS(IsDefined(PCollectionNodes_[input].RowVtable));

        CheckPCollectionType(input, "ParDo being connected input", tags[0].GetRowVtable());
    }

    std::vector<TPCollectionNodeId> outputs;
    const auto& outputTags = parDo->GetOutputTags();
    outputs.reserve(outputTags.size());
    for (const auto& tag : outputTags) {
        outputs.push_back(AddPCollectionNode(tag.GetRowVtable()));
    }

    ParDoNodes_.push_back(TParDoNode{
        .ParDo = std::move(parDo),
        .Inputs = std::move(inputs),
        .Outputs = outputs,
    });

    return outputs;
}

TParDoTreeBuilder::TPCollectionNodeId TParDoTreeBuilder::AddParDoVerifySingleOutput(IRawParDoPtr parDo, TPCollectionNodeId input)
{
    auto result = AddParDo(std::move(parDo), input);
    Y_ABORT_UNLESS(result.size() == 1, "Expected single output, actual output count: %d", static_cast<int>(result.size()));
    return result[0];
}

void TParDoTreeBuilder::AddParDoVerifyNoOutput(IRawParDoPtr parDo, TPCollectionNodeId input)
{
    auto result = AddParDo(std::move(parDo), input);
    Y_ABORT_UNLESS(result.empty(), "Expected no output, actual output count: %d", static_cast<int>(result.size()));
}

std::vector<TParDoTreeBuilder::TPCollectionNodeId> TParDoTreeBuilder::AddParDoChain(TPCollectionNodeId input, const std::vector<IRawParDoPtr>& parDoList)
{
    auto result = std::vector({input});

    for (const auto& parDo : parDoList) {
        Y_ABORT_UNLESS(result.size() == 1);
        result = AddParDo(parDo, result[0]);
    }

    return result;
}

TParDoTreeBuilder::TPCollectionNodeId TParDoTreeBuilder::AddParDoChainVerifySingleOutput(TPCollectionNodeId input, const std::vector<IRawParDoPtr>& parDoList)
{
    auto result = AddParDoChain(input, parDoList);
    Y_ABORT_UNLESS(result.size() == 1, "Expected single output, actual output count: %d", static_cast<int>(result.size()));
    return result[0];
}

void TParDoTreeBuilder::AddParDoChainVerifyNoOutput(TPCollectionNodeId input, const std::vector<IRawParDoPtr>& parDoList)
{
    auto result = AddParDoChain(input, parDoList);
    Y_ABORT_UNLESS(result.empty(), "Expected no output, actual output count: %d", static_cast<int>(result.size()));
}

TParDoTreeBuilder::TPCollectionNodeId TParDoTreeBuilder::AddPCollectionNode(const TRowVtable& rowVtable)
{
    Y_ABORT_UNLESS(!Built_);

    auto result = std::ssize(PCollectionNodes_);
    PCollectionNodes_.push_back({.GlobalOutputIndex=InvalidOutputIndex, .RowVtable=rowVtable});
    return result;
}

void TParDoTreeBuilder::CheckPCollectionType(int nodeId, TStringBuf expectedDescription, const TRowVtable& expectedRowVtable)
{
    Y_ABORT_UNLESS(0 <= nodeId && nodeId < std::ssize(PCollectionNodes_));
    const auto& pCollectionRowVtable = PCollectionNodes_[nodeId].RowVtable;
    if (pCollectionRowVtable.TypeName != expectedRowVtable.TypeName) {
        TStringStream error;
        error
            << "Type mismatch. "
            << "Node " << nodeId << " type: " << pCollectionRowVtable.TypeName
            << " " << expectedDescription
            << " type: " << expectedRowVtable.TypeName;
        Y_ABORT("%s", error.Str().c_str());
    }
}

void TParDoTreeBuilder::MarkAsOutput(TPCollectionNodeId nodeId, const TDynamicTypeTag& tag)
{
    Y_ABORT_IF(Built_);
    Y_ABORT_IF(nodeId < 0 || nodeId >= std::ssize(PCollectionNodes_));
    Y_ABORT_IF(PCollectionNodes_[nodeId].GlobalOutputIndex != InvalidOutputIndex); //GlobalOutputIndex allready set
    Y_ABORT_IF(nodeId == RootNodeId); //RootNode can't be a GlobalOutput

    PCollectionNodes_[nodeId].GlobalOutputIndex = std::ssize(MarkedOutputTypeTags_);
    MarkedOutputTypeTags_.push_back(tag);
    if (tag) {
        CheckPCollectionType(nodeId, "marked output tag", tag.GetRowVtable());
    }
}

void TParDoTreeBuilder::MarkAsOutputs(const std::vector<TPCollectionNodeId>& nodeIds)
{
    for (auto nodeId : nodeIds) {
        MarkAsOutput(nodeId);
    }
}

IParDoTreePtr TParDoTreeBuilder::Build()
{
    Y_ABORT_UNLESS(!Built_);
    Y_ABORT_UNLESS(!ParDoNodes_.empty());

    CheckNoHangingPCollectionNodes();

    Built_ = true;
    return NYT::New<TParDoTree>(
        std::move(ParDoNodes_),
        PCollectionNodes_,
        std::move(MarkedOutputTypeTags_));
}

const TParDoTreeBuilder::TParDoNode& TParDoTreeBuilder::FindParDoByOutput(int pCollectionIndex) const noexcept
{
    for (const TParDoNode& node : ParDoNodes_) {
        if (node.Outputs.end() != std::find(node.Outputs.begin(), node.Outputs.end(), pCollectionIndex)) {
            return node;
        }
    }
    Y_ABORT();
}

void TParDoTreeBuilder::CheckNoHangingPCollectionNodes() const
{
    auto index_of = [] (const auto& container, const auto& v) {
        return std::distance(container.begin(), std::find(container.begin(), container.end(), v));
    };
    THashSet<TPCollectionNodeId> parDoInputs;
    for (const TParDoNode& parDoNode : ParDoNodes_) {
        for (const auto input : parDoNode.Inputs) {
            parDoInputs.insert(input);
        }
    }
    for (ssize_t pCollectionIndex = 0; pCollectionIndex < std::ssize(PCollectionNodes_); ++pCollectionIndex) {
        const bool isGlobalOutput = PCollectionNodes_[pCollectionIndex].GlobalOutputIndex != InvalidOutputIndex;
        if (!isGlobalOutput && !parDoInputs.contains(pCollectionIndex)) {
            const TParDoNode& parDoNode = FindParDoByOutput(pCollectionIndex);
            const ssize_t outputIndex = index_of(parDoNode.Outputs, pCollectionIndex);
            const TString name = NPrivate::GetAttributeOrDefault(*parDoNode.ParDo, TransformNameTag, {"<unknown>"});
            Y_ABORT("Transform '%s' has unused output No %zd", name.c_str(), outputIndex);
        }
    }
}


THashMap<TParDoTreeBuilder::TPCollectionNodeId, TParDoTreeBuilder::TPCollectionNodeId> TParDoTreeBuilder::Fuse(
    const TParDoTreeBuilder& other,
    TPCollectionNodeId input)
{
    THashMap<TPCollectionNodeId, TPCollectionNodeId> otherToThisMap;
    THashMap<TPCollectionNodeId, TPCollectionNodeId> thisToOtherMap;
    auto link = [&] (TPCollectionNodeId otherId, TPCollectionNodeId thisId) {
        bool inserted;
        inserted = otherToThisMap.emplace(otherId, thisId).second;
        Y_ABORT_UNLESS(inserted);
        inserted = thisToOtherMap.emplace(thisId, otherId).second;
        Y_ABORT_UNLESS(inserted);
    };
    link(0, input);

    for (const auto& otherParDoNode : other.ParDoNodes_) {
        std::vector<TPCollectionNodeId> thisInputIds;
        for (const auto input : otherParDoNode.Inputs) {
            auto it = otherToThisMap.find(input);
            Y_ABORT_UNLESS(it != otherToThisMap.end());
            thisInputIds.push_back(it->second);
        }

        auto thisOutputIds = AddParDo(otherParDoNode.ParDo, thisInputIds);
        Y_ABORT_UNLESS(thisOutputIds.size() == otherParDoNode.Outputs.size());
        for (ssize_t i = 0; i < std::ssize(thisOutputIds); ++i) {
            auto otherId = otherParDoNode.Outputs[i];
            auto thisId = thisOutputIds[i];
            link(otherId, thisId);
        }
    }
    return otherToThisMap;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
