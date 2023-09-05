#include "par_do_tree.h"

#include "fn_attributes_ops.h"

#include "../fns.h"

#include <yt/cpp/roren/interface/execution_context.h>

#include <util/generic/iterator_range.h>
#include <util/generic/overloaded.h>

#include <util/string/builder.h>
#include <util/generic/hash_set.h>

#include <map>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TPipePCollectionNode
    : public IRawOutput
{
public:
    TPipePCollectionNode(std::vector<IRawOutputPtr> outputs, std::vector<IRawParDoPtr> parDos)
        : Outputs_(std::move(outputs))
        , ParDos_(std::move(parDos))
    { }

    void AddRaw(const void* rows, ssize_t count) override
    {
        for (const auto& output : Outputs_) {
            output->AddRaw(rows, count);
        }
        for (const auto& parDo : ParDos_) {
            parDo->Do(rows, count);
        }
    }

    void Close() override
    { }

private:
    std::vector<IRawOutputPtr> Outputs_;
    std::vector<IRawParDoPtr> ParDos_;
};

using TPipePCollectionNodePtr = ::TIntrusivePtr<TPipePCollectionNode>;

////////////////////////////////////////////////////////////////////////////////

void TParDoTreeBuilder::TParDoNode::Save(IOutputStream* os) const
{
    SaveSerializable(os, ParDo);
    ::Save(os, Input);
    ::Save(os, Outputs);
}

void TParDoTreeBuilder::TParDoNode::Load(IInputStream* is)
{
    LoadSerializable(is, ParDo);
    ::Load(is, Input);
    ::Load(is, Outputs);
}

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

    void Finish() override
    {
        for (const auto& parDoNode : ParDoTopoOrder()) {
            parDoNode.ParDo->Finish();
        }
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TParDoTree>();
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
        std::map<TString, int> Names_;
        for (const auto& node : ParDoNodes_) {
            if (const auto name = node.ParDo->GetFnAttributes().GetName()) {
                Names_[*name] += 1;
            } else {
                Names_[{}] += 1;
            }
        }

        TStringStream out;
        bool first = true;
        out << "TParDoTree{";
        for (const auto& [name, count] : Names_) {
            if (first) {
                first = false;
            } else {
                out << ", ";
            }
            if (name.empty()) {
                out << "<unknown>";
            } else {
                out << name;
            }
            if (count != 1) {
                out << " * " << count;
            }
        }
        out << "}";
        return out.Str();
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
                    Y_VERIFY(0 <= globalOutputIndex && globalOutputIndex < std::ssize(OutputTags_));
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
            pipePCollectionNodeOutputs[parDoNode.Input].second.push_back(parDoNode.ParDo);
        }

        for (int pCollectionIndex = 0; pCollectionIndex < std::ssize(PCollectionNodeOutputIndex_); ++pCollectionIndex) {
            auto globalOutputIndex = PCollectionNodeOutputIndex_[pCollectionIndex];
            if (globalOutputIndex != InvalidOutputIndex) {
                auto& pCollectionNodeOutputs = pipePCollectionNodeOutputs[pCollectionIndex];
                pCollectionNodeOutputs.first.emplace_back(globalOutputs[globalOutputIndex]);
            }
        }

        PipePCollectionNodes_.clear();
        PipePCollectionNodes_.reserve(pipePCollectionNodeOutputs.size());
        for (auto& [outputs, parDos] : pipePCollectionNodeOutputs) {
            auto node = ::MakeIntrusive<TPipePCollectionNode>(std::move(outputs), std::move(parDos));
            PipePCollectionNodes_.push_back(std::move(node));
        }
    }

    void StartParDos(const IExecutionContextPtr& context)
    {
        Y_VERIFY(PipePCollectionNodes_.size() == PCollectionNodeOutputIndex_.size());

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
        for (const auto& node : parDoNodeList) {
            const auto& currentAttributes = node.ParDo->GetFnAttributes();
            TFnAttributesOps::Merge(result, currentAttributes);
        }
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
    Y_VERIFY(0 <= input && input < std::ssize(PCollectionNodes_));
    const auto& tags = parDo->GetInputTags();
    if (input == 0) {
        Y_VERIFY(tags.size() == 1);
        if (!IsDefined(PCollectionNodes_[0].RowVtable)) {
            PCollectionNodes_[0].RowVtable = tags[0].GetRowVtable();
        }
    }
    Y_VERIFY(IsDefined(PCollectionNodes_[input].RowVtable));

    CheckPCollectionType(input, tags[0].GetRowVtable());

    std::vector<TPCollectionNodeId> outputs;
    const auto& outputTags = parDo->GetOutputTags();
    outputs.reserve(outputTags.size());
    for (const auto& tag : outputTags) {
        outputs.push_back(AddPCollectionNode(tag.GetRowVtable()));
    }

    ParDoNodes_.push_back(TParDoNode{
        .ParDo = std::move(parDo),
        .Input = input,
        .Outputs = outputs,
    });

    return outputs;
}

TParDoTreeBuilder::TPCollectionNodeId TParDoTreeBuilder::AddPCollectionNode(const TRowVtable& rowVtable)
{
    Y_VERIFY(!Built_);

    auto result = std::ssize(PCollectionNodes_);
    PCollectionNodes_.push_back({.GlobalOutputIndex=InvalidOutputIndex, .RowVtable=rowVtable});
    return result;
}

void TParDoTreeBuilder::CheckPCollectionType(int nodeId, const TRowVtable& rowVtable)
{
    Y_VERIFY(0 <= nodeId && nodeId < std::ssize(PCollectionNodes_));
    const auto& pCollectionRowVtable = PCollectionNodes_[nodeId].RowVtable;
    if (pCollectionRowVtable.TypeName != rowVtable.TypeName) {
        ythrow yexception() << "Type mismatch. Checked type: " << rowVtable.TypeName << "Node " << nodeId << " type " << pCollectionRowVtable.TypeName;
    }
}

void TParDoTreeBuilder::MarkAsOutput(TPCollectionNodeId nodeId, const TDynamicTypeTag& tag)
{
    Y_VERIFY(!Built_);

    Y_VERIFY(0 <= nodeId && nodeId < std::ssize(PCollectionNodes_));
    Y_VERIFY(PCollectionNodes_[nodeId].GlobalOutputIndex == InvalidOutputIndex);
    Y_VERIFY(nodeId != RootNodeId);
    PCollectionNodes_[nodeId].GlobalOutputIndex = std::ssize(MarkedOutputTypeTags_);
    MarkedOutputTypeTags_.push_back(tag);
    if (tag) {
        CheckPCollectionType(nodeId, tag.GetRowVtable());
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
    Y_VERIFY(!Built_);
    Y_VERIFY(!ParDoNodes_.empty());

    CheckNoHangingPCollectionNodes();

    Built_ = true;
    return ::MakeIntrusive<TParDoTree>(
        std::move(ParDoNodes_),
        PCollectionNodes_,
        std::move(MarkedOutputTypeTags_));
}

void TParDoTreeBuilder::CheckNoHangingPCollectionNodes() const
{
    THashSet<TPCollectionNodeId> parDoInputs;
    for (const auto& parDoNode : ParDoNodes_) {
        parDoInputs.insert(parDoNode.Input);
    }
    for (auto pCollectionIndex = 0; pCollectionIndex < std::ssize(PCollectionNodes_); ++pCollectionIndex) {
        auto isGlobalOutput = PCollectionNodes_[pCollectionIndex].GlobalOutputIndex != InvalidOutputIndex;
        Y_VERIFY(isGlobalOutput || parDoInputs.contains(pCollectionIndex));
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
        Y_VERIFY(inserted);
        inserted = thisToOtherMap.emplace(thisId, otherId).second;
        Y_VERIFY(inserted);
    };
    link(0, input);

    for (const auto& otherParDoNode : other.ParDoNodes_) {
        auto it = otherToThisMap.find(otherParDoNode.Input);
        Y_VERIFY(it != otherToThisMap.end());
        auto thisInputId = it->second;

        auto thisOutputIds = AddParDo(otherParDoNode.ParDo, thisInputId);
        Y_VERIFY(thisOutputIds.size() == otherParDoNode.Outputs.size());
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
