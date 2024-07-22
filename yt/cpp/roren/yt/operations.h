#pragma once

#include "connectors.h"
#include "node.h"
#include "yt_io_private.h"
#include "yt_graph_v2.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

using TOperationNode = TYtGraphV2::TOperationNode;
using TOperationNodePtr = TYtGraphV2::TOperationNodePtr;

////////////////////////////////////////////////////////////////////////////////

enum class EOperationType
{
    Map,
    MapReduce,
    Merge,
    Sort,
};

////////////////////////////////////////////////////////////////////////////////

class TYtGraphV2::TOperationNode
    : public TNodeHierarchyRoot<TYtGraphV2::TOperationNode>
{
public:
    std::vector<TTableNode*> InputTables;
    THashMap<TOperationConnector, TTableNode*> OutputTables;

public:
    TOperationNode(TString firstName, THashSet<TString> transformNames)
        : FirstName_(std::move(firstName))
        , TransformNames_(std::move(transformNames))
    {
        Y_ABORT_UNLESS(!FirstName_.empty());
    }

    virtual ~TOperationNode() = default;

    virtual EOperationType GetOperationType() const = 0;

    virtual std::pair<TParDoTreeBuilder*, TParDoTreeBuilder::TPCollectionNodeId> ResolveOperationConnector(const TOperationConnector& connector) = 0;

    TTableNode* VerifiedGetSingleInput() const
    {
        Y_ABORT_UNLESS(InputTables.size() == 1);
        return InputTables[0];
    }

    const TString& GetFirstName() const
    {
        return FirstName_;
    }

    const THashSet<TString>& GetTransformNames() const
    {
        return TransformNames_;
    }

private:
    // Клониурет вершину, сохраняя пользовательскую логику операции привязанную к ней, но не клонирует графовую структуру.
    virtual std::shared_ptr<TOperationNode> Clone() const = 0;

    void MergeTransformNames(const THashSet<TString>& other)
    {
        return TransformNames_.insert(other.begin(), other.end());
    }

private:
    const TString FirstName_;
    THashSet<TString> TransformNames_;

    friend class TPlainGraph;
    friend class TMapReduceOperationNode;
    friend class TMapOperationNode;
};

////////////////////////////////////////////////////////////////////////////////

class TMergeOperationNode
    : public TYtGraphV2::TOperationNode
{
public:
    EOperationType GetOperationType() const override
    {
        return EOperationType::Merge;
    }

    std::shared_ptr<TOperationNode> Clone() const override
    {
        auto result = TMergeOperationNode::MakeShared(GetFirstName(), GetTransformNames());
        return result;
    }

    std::pair<TParDoTreeBuilder*, TParDoTreeBuilder::TPCollectionNodeId> ResolveOperationConnector(const TOperationConnector&) override
    {
        return {nullptr, 0};
    }

    static std::shared_ptr<TMergeOperationNode> MakeShared(TString firstName, THashSet<TString> transformNames)
    {
        return std::shared_ptr<TMergeOperationNode>(new TMergeOperationNode(std::move(firstName), std::move(transformNames)));
    }

private:
    explicit TMergeOperationNode(TString firstName, THashSet<TString> transformNames)
        : TYtGraphV2::TOperationNode(firstName, std::move(transformNames))
    { }
};

////////////////////////////////////////////////////////////////////////////////

class TMapOperationNode
    : public TYtGraphV2::TOperationNode
{
public:
    EOperationType GetOperationType() const override
    {
        return EOperationType::Map;
    }

    std::shared_ptr<TOperationNode> Clone() const override
    {
        auto result = TMapOperationNode::MakeShared(GetFirstName(), GetTransformNames());
        result->MapperBuilder_ = MapperBuilder_;
        return result;
    }

    std::pair<TParDoTreeBuilder*, TParDoTreeBuilder::TPCollectionNodeId> ResolveOperationConnector(const TOperationConnector& connector) override
    {
        Y_ABORT_UNLESS(std::holds_alternative<TMapperOutputConnector>(connector));
        const auto& mapperConnector = std::get<TMapperOutputConnector>(connector);
        Y_ABORT_UNLESS(mapperConnector.MapperIndex == 0);
        return {&MapperBuilder_, mapperConnector.NodeId};
    }

    const TParDoTreeBuilder& GetMapperBuilder() const
    {
        return MapperBuilder_;
    }

    THashMap<TParDoTreeBuilder::TPCollectionNodeId, TParDoTreeBuilder::TPCollectionNodeId>
    Fuse(const TMapOperationNode& other, TParDoTreeBuilder::TPCollectionNodeId fusionPoint)
    {
        MergeTransformNames(other.TransformNames_);
        auto result = MapperBuilder_.Fuse(other.MapperBuilder_, fusionPoint);
        return result;
    }

private:
    explicit TMapOperationNode(TString firstName, THashSet<TString> transformNames)
        : TYtGraphV2::TOperationNode(firstName, std::move(transformNames))
    { }

    static std::shared_ptr<TMapOperationNode> MakeShared(TString firstName, THashSet<TString> transformNames)
    {
        return std::shared_ptr<TMapOperationNode>(new TMapOperationNode(std::move(firstName), std::move(transformNames)));
    }

private:
    TParDoTreeBuilder MapperBuilder_;

    friend class TYtGraphV2::TPlainGraph;
    friend class TMapReduceOperationNode;
};

////////////////////////////////////////////////////////////////////////////////

class TSortOperationNode
    : public TYtGraphV2::TOperationNode
{
public:
    EOperationType GetOperationType() const override
    {
        return EOperationType::Sort;
    }

    std::shared_ptr<TOperationNode> Clone() const override
    {
        auto result = TSortOperationNode::MakeShared(GetFirstName(), GetTransformNames());
        result->Write_ = Write_;
        return result;
    }

    std::pair<TParDoTreeBuilder*, TParDoTreeBuilder::TPCollectionNodeId> ResolveOperationConnector(const TOperationConnector&) override
    {
        return {nullptr, 0};
    }

    const IRawYtSortedWrite* GetWrite() const
    {
        return Write_;
    }

private:
    TSortOperationNode(
        TString firstName,
        THashSet<TString> transforms)
        : TYtGraphV2::TOperationNode(firstName, std::move(transforms))
    { }

    static std::shared_ptr<TSortOperationNode> MakeShared(TString firstName, THashSet<TString> transformNames)
    {
        return std::shared_ptr<TSortOperationNode>(new TSortOperationNode(std::move(firstName), std::move(transformNames)));
    }

private:
    IRawYtSortedWrite* Write_;

    friend class TYtGraphV2::TPlainGraph;
};

////////////////////////////////////////////////////////////////////////////////

class TMapReduceOperationNode
    : public TYtGraphV2::TOperationNode
{
public:
    EOperationType GetOperationType() const override
    {
        return EOperationType::MapReduce;
    }

    std::shared_ptr<TOperationNode> Clone() const override
    {
        auto result = TMapReduceOperationNode::MakeShared(GetFirstName(), GetTransformNames(), ssize(MapperBuilderList_));
        result->MapperBuilderList_ = MapperBuilderList_;
        result->CombinerBuilder_ = CombinerBuilder_;
        result->ReducerBuilder_ = ReducerBuilder_;
        return result;
    }

    std::pair<TParDoTreeBuilder*, TParDoTreeBuilder::TPCollectionNodeId> ResolveOperationConnector(const TOperationConnector& connector) override
    {
        return std::visit([&] (const auto& connector) -> std::pair<TParDoTreeBuilder*, TParDoTreeBuilder::TPCollectionNodeId> {
            using TType = std::decay_t<decltype(connector)>;
            if constexpr (std::is_same_v<TType, TMergeOutputConnector>) {
                Y_ABORT("Cannot resolve operation connector for TMergeOutputConnector");
            } else if constexpr (std::is_same_v<TType, TSortOutputConnector>) {
                Y_ABORT("Cannot resolve operation connector for TSortOutputConnector");
            } else if constexpr (std::is_same_v<TType, TStatefulConnector>) {
                Y_ABORT("Cannot resolve operation connector for TStatefulConnector");
            } else if constexpr (std::is_same_v<TType, TMapperOutputConnector>){
                Y_ABORT_UNLESS(connector.MapperIndex >= 0);
                Y_ABORT_UNLESS(connector.MapperIndex < std::ssize(MapperBuilderList_));
                return std::pair{&MapperBuilderList_[connector.MapperIndex], connector.NodeId};
            } else if constexpr (std::is_same_v<TType, TReducerOutputConnector>) {
                return std::pair{&ReducerBuilder_, connector.NodeId};
            } else {
                static_assert(TDependentFalse<TType>);
            }
        }, connector);
    }

    // Fuses a map operation that runs before current MapReduce operation into this MapReduce operation.
    // All outputs of the map operation are relinked to MapReduce operation.
    // Map operation becomes orphan.
    THashMap<TParDoTreeBuilder::TPCollectionNodeId, TParDoTreeBuilder::TPCollectionNodeId>
    FusePrecedingMapper(ssize_t mapperIndex);

    THashMap<TParDoTreeBuilder::TPCollectionNodeId, TParDoTreeBuilder::TPCollectionNodeId>
    FuseToReducer(const TMapOperationNode& other, TParDoTreeBuilder::TPCollectionNodeId fusionPoint)
    {
        MergeTransformNames(other.TransformNames_);
        auto result = ReducerBuilder_.Fuse(other.MapperBuilder_, fusionPoint);
        return result;
    }

    const std::vector<TParDoTreeBuilder>& GetMapperBuilderList() const
    {
        return MapperBuilderList_;
    }

    const TParDoTreeBuilder& GetCombinerBuilder() const
    {
        return CombinerBuilder_;
    }

    const TParDoTreeBuilder& GetReducerBuilder() const
    {
        return ReducerBuilder_;
    }

private:
    TMapReduceOperationNode(
        TString firstName,
        THashSet<TString> transforms,
        ssize_t inputCount)
        : TYtGraphV2::TOperationNode(firstName, std::move(transforms))
        , MapperBuilderList_(inputCount)
    {
        Y_ABORT_UNLESS(inputCount >= 1);
    }

    static std::shared_ptr<TMapReduceOperationNode> MakeShared(TString firstName, THashSet<TString> transformNames, ssize_t inputCount)
    {
        return std::shared_ptr<TMapReduceOperationNode>(new TMapReduceOperationNode(std::move(firstName), std::move(transformNames), inputCount));
    }

private:
    std::vector<TParDoTreeBuilder> MapperBuilderList_;
    TParDoTreeBuilder CombinerBuilder_;
    TParDoTreeBuilder ReducerBuilder_;

    friend class TYtGraphV2::TPlainGraph;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
