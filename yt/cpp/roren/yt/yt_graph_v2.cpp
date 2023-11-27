#include "yt_graph_v2.h"
#include "jobs.h"

#include "yt_graph.h"
#include "yt_io_private.h"
#include "yt_proto_io.h"

#include <yt/cpp/roren/yt/proto/config.pb.h>
#include <yt/cpp/roren/yt/proto/kv.pb.h>

#include <yt/cpp/roren/interface/private/par_do_tree.h>
#include <yt/cpp/roren/interface/roren.h>

#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/client.h>

#include <library/cpp/yt/string/format.h>

#include <util/generic/hash_multi_map.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

struct TMergeOutputConnector
{
    bool operator==(const TMergeOutputConnector& other) const = default;
};

struct TMapperOutputConnector
{
    // Index of a mapper inside operation
    ssize_t MapperIndex = 0;
    TParDoTreeBuilder::TPCollectionNodeId NodeId = TParDoTreeBuilder::RootNodeId;

    bool operator==(const TMapperOutputConnector& other) const = default;
};

struct TReducerOutputConnector
{
    TParDoTreeBuilder::TPCollectionNodeId NodeId = TParDoTreeBuilder::RootNodeId;

    bool operator==(const TReducerOutputConnector& other) const = default;
};

using TOperationConnector = std::variant<TMergeOutputConnector, TMapperOutputConnector, TReducerOutputConnector>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate

template <>
struct THash<NRoren::NPrivate::TOperationConnector>
{
    size_t operator() (const NRoren::NPrivate::TOperationConnector& connector) {
        using namespace NRoren::NPrivate;
        return std::visit([] (const auto& connector) -> size_t {
            using TType = std::decay_t<decltype(connector)>;
            if constexpr (std::is_same_v<TType, TMergeOutputConnector>) {
                // Carefuly and randomly generated number.
                return 1933246098;
            } else if constexpr (std::is_same_v<TType, TMapperOutputConnector>) {
                auto val = std::tuple{0, connector.MapperIndex, connector.NodeId};
                return THash<decltype(val)>()(val);
            } else if constexpr (std::is_same_v<TType, TReducerOutputConnector>) {
                auto val = std::tuple{1, connector.NodeId};
                return THash<decltype(val)>()(val);
            } else {
                static_assert(std::is_same_v<TType, TMergeOutputConnector>);
            }
        }, connector);
    }
};

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

using TTableNode = TYtGraphV2::TTableNode;
using TTableNodePtr = TYtGraphV2::TTableNodePtr;

using TOperationNode = TYtGraphV2::TOperationNode;
using TOperationNodePtr = TYtGraphV2::TOperationNodePtr;

////////////////////////////////////////////////////////////////////////////////

enum class EOperationType
{
    Map,
    MapReduce,
    Merge,
};

enum class EJobType
{
    Map,
    Reduce,
};

enum class ETableType
{
    Input,
    Output,
    Intermediate,
};

enum class ETableFormat
{
    TNode,
    Proto,
};

////////////////////////////////////////////////////////////////////////////////

class TInputTableNode;
class TOutputTableNode;

////////////////////////////////////////////////////////////////////////////////

const TPCollectionNode* VerifiedGetSingleSource(const TTransformNodePtr& transform)
{
    Y_ABORT_UNLESS(transform->GetSourceCount() == 1);
    return transform->GetSource(0).Get();
}

const TPCollectionNode* VerifiedGetSingleSink(const TTransformNodePtr& transform)
{
    Y_ABORT_UNLESS(transform->GetSinkCount() == 1);
    return transform->GetSink(0).Get();
}

////////////////////////////////////////////////////////////////////////////////

TParDoTreeBuilder::TPCollectionNodeId VerifiedGetNodeIdOfMapperConnector(const TOperationConnector& connector)
{
    Y_ABORT_UNLESS(std::holds_alternative<TMapperOutputConnector>(connector));
    const auto& mapperConnector = std::get<TMapperOutputConnector>(connector);
    Y_ABORT_UNLESS(mapperConnector.MapperIndex == 0);
    return mapperConnector.NodeId;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
class TNodeHierarchyRoot
{
public:
    virtual ~TNodeHierarchyRoot() = default;

    template <typename T>
    T* VerifiedAsPtr()
    {
        static_assert(std::is_base_of_v<TDerived, T>, "Class is not derived from TNodeHierarchyRoot");
        auto* result = dynamic_cast<T*>(this);
        Y_ABORT_UNLESS(result);
        return result;
    }

    template <typename T>
    const T* VerifiedAsPtr() const
    {
        static_assert(std::is_base_of_v<TDerived, T>, "Class is not derived from TNodeHierarchyRoot");
        const auto* result = dynamic_cast<const T*>(this);
        Y_ABORT_UNLESS(result);
        return result;
    }

    template <typename T>
    T* TryAsPtr()
    {
        return dynamic_cast<T*>(this);
    }

    template <typename T>
    const T* TryAsPtr() const
    {
        return dynamic_cast<const T*>(this);
    }
};

class TAddTableIndexDoFn
    : public IDoFn<NYT::TNode, NYT::TNode>
{
public:
    TAddTableIndexDoFn() = default;

    TAddTableIndexDoFn(ssize_t tableIndex)
        : TableIndex_(tableIndex)
    {
        Y_ABORT_UNLESS(tableIndex >= 0);
    }

    void Do(const NYT::TNode& row, TOutput<NYT::TNode>& output) override
    {
        auto result = row;
        result["table_index"] = TableIndex_;
        output.Add(result);
    }

private:
    ssize_t TableIndex_ = 0;
    Y_SAVELOAD_DEFINE_OVERRIDE(TableIndex_);
};

class TAddTableIndexToProtoDoFn
    : public IDoFn<TKVProto, TKVProto>
{
public:
    TAddTableIndexToProtoDoFn() = default;

    TAddTableIndexToProtoDoFn(ssize_t tableIndex)
        : TableIndex_(tableIndex)
    {
        Y_ABORT_UNLESS(tableIndex >= 0);
    }

    void Do(const TKVProto& row, TOutput<TKVProto>& output) override
    {
        auto result = row;
        result.SetTableIndex(TableIndex_);
        output.Add(result);
    }

private:
    ssize_t TableIndex_ = 0;
    Y_SAVELOAD_DEFINE_OVERRIDE(TableIndex_);
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

class TYtGraphV2::TTableNode
    : public TNodeHierarchyRoot<TYtGraphV2::TTableNode>
{
public:
    TRowVtable Vtable;

    struct TInputFor
    {
        TOperationNode* Operation = nullptr;
        ssize_t Index = 0;
        bool operator == (const TInputFor& other) const = default;
    };
    std::vector<TInputFor> InputFor;

    struct TOutputOf {
        TOperationNode* Operation = nullptr;
        TOperationConnector Connector = TMapperOutputConnector{};
    } OutputOf = {};

public:
    explicit TTableNode(TRowVtable vtable)
        : Vtable(vtable)
    { }

    virtual ~TTableNode() = default;

    virtual ETableType GetTableType() const = 0;
    virtual ETableFormat GetTableFormat() const = 0;

    virtual IRawParDoPtr CreateDecodingParDo() const = 0;

    virtual IRawParDoPtr CreateWriteParDo(ssize_t tableIndex) const = 0;
    virtual IRawParDoPtr CreateEncodingParDo() const = 0;

    virtual NYT::TRichYPath GetPath() const = 0;

    virtual const ::google::protobuf::Descriptor* GetProtoDescriptor() const = 0;

private:
    virtual std::shared_ptr<TTableNode> Clone() const = 0;

    friend class TYtGraphV2::TPlainGraph;
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
        result->ReducerBuilder_ = ReducerBuilder_;
        return result;
    }

    std::pair<TParDoTreeBuilder*, TParDoTreeBuilder::TPCollectionNodeId> ResolveOperationConnector(const TOperationConnector& connector) override
    {
        return std::visit([&] (const auto& connector) -> std::pair<TParDoTreeBuilder*, TParDoTreeBuilder::TPCollectionNodeId> {
            using TType = std::decay_t<decltype(connector)>;
            if constexpr (std::is_same_v<TType, TMergeOutputConnector>) {
                Y_ABORT("Cannot resolve operation connector for TMergeOutputConnector");
            } else if constexpr (std::is_same_v<TType, TMapperOutputConnector>){
                Y_ABORT_UNLESS(connector.MapperIndex >= 0);
                Y_ABORT_UNLESS(connector.MapperIndex < std::ssize(MapperBuilderList_));
                return std::pair{&MapperBuilderList_[connector.MapperIndex], connector.NodeId};
            } else if constexpr (std::is_same_v<TType, TReducerOutputConnector>) {
                return std::pair{&ReducerBuilder_, connector.NodeId};
            } else {
                static_assert(std::is_same_v<TType, TMergeOutputConnector>);
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

class TInputTableNode
    : public TYtGraphV2::TTableNode
{
public:
    TInputTableNode(TRowVtable vtable, IRawYtReadPtr rawYtRead)
        : TYtGraphV2::TTableNode(std::move(vtable))
        , RawYtRead_(rawYtRead)
    { }

    ETableType GetTableType() const override
    {
        return ETableType::Input;
    }

    ETableFormat GetTableFormat() const override
    {
        return GetProtoDescriptor() ? ETableFormat::Proto : ETableFormat::TNode;
    }

    NYT::TRichYPath GetPath() const override
    {
        return RawYtRead_->GetPath();
    }

    IRawParDoPtr CreateDecodingParDo() const override
    {
        auto protoDecodingParDo = NPrivate::GetAttribute(*RawYtRead_, DecodingParDoTag);

        if (protoDecodingParDo) {
            return *protoDecodingParDo;
        } else {
            return MakeRawIdComputation(MakeRowVtable<NYT::TNode>());
        }
    }

    IRawParDoPtr CreateWriteParDo(ssize_t) const override
    {
        Y_ABORT("TInputTableNode is not expected to be written into");
    }

    IRawParDoPtr CreateEncodingParDo() const override
    {
        Y_ABORT("TInputTableNode is not expected to be written into");
    }

    const ::google::protobuf::Descriptor* GetProtoDescriptor() const override
    {
        auto descriptorPtr = NPrivate::GetAttribute(
            *RawYtRead_,
            ProtoDescriptorTag
        );
        return descriptorPtr ? *descriptorPtr : nullptr;
    }

private:
    std::shared_ptr<TTableNode> Clone() const override
    {
        return std::make_shared<TInputTableNode>(Vtable, RawYtRead_);
    }

private:
    const IRawYtReadPtr RawYtRead_;
};

////////////////////////////////////////////////////////////////////////////////

class TOutputTableNode
    : public TYtGraphV2::TTableNode
{
public:
    TOutputTableNode(TRowVtable vtable, IRawYtWritePtr rawYtWrite)
        : TYtGraphV2::TTableNode(std::move(vtable))
        , RawYtWrite_(std::move(rawYtWrite))
    { }

    ETableType GetTableType() const override
    {
        return ETableType::Output;
    }

    ETableFormat GetTableFormat() const override
    {
        return GetProtoDescriptor() ? ETableFormat::Proto : ETableFormat::TNode;
    }

    IRawParDoPtr CreateDecodingParDo() const override
    {
        auto protoDecodingParDo = NPrivate::GetAttribute(*RawYtWrite_, DecodingParDoTag);

        if (protoDecodingParDo) {
            return *protoDecodingParDo;
        } else {
            return MakeRawIdComputation(MakeRowVtable<NYT::TNode>());
        }
    }

    IRawParDoPtr CreateWriteParDo(ssize_t tableIndex) const override
    {
        auto writeParDo = NPrivate::GetAttribute(*RawYtWrite_, WriteParDoTag);

        if (writeParDo) {
            auto mutableParDo = (*writeParDo)->Clone();

            auto upcastedParDo = VerifyDynamicCast<IProtoIOParDo*>(mutableParDo.Get());
            upcastedParDo->SetTableIndex(tableIndex);

            return mutableParDo;
        } else {
            return CreateWriteNodeParDo(tableIndex);
        }
    }

    IRawParDoPtr CreateEncodingParDo() const override
    {
        auto protoEncodingParDo = NPrivate::GetAttribute(*RawYtWrite_, EncodingParDoTag);

        if (protoEncodingParDo) {
            return *protoEncodingParDo;
        } else {
            return MakeRawIdComputation(MakeRowVtable<NYT::TNode>());
        }
    }

    NYT::TRichYPath GetPath() const override
    {
        auto path = RawYtWrite_->GetPath();
        path.Schema(RawYtWrite_->GetSchema());
        if (!path.OptimizeFor_.Defined()) {
            path.OptimizeFor_ = NYT::EOptimizeForAttr::OF_SCAN_ATTR;
        }
        return path;
    }

    const ::google::protobuf::Descriptor* GetProtoDescriptor() const override
    {
        auto descriptorPtr = NPrivate::GetAttribute(
            *RawYtWrite_,
            ProtoDescriptorTag
        );
        return descriptorPtr ? *descriptorPtr : nullptr;
    }

    const NYT::TTableSchema& GetSchema() const
    {
        return RawYtWrite_->GetSchema();
    }

    std::vector<std::pair<NYT::TRichYPath, NYT::TTableSchema>> GetPathSchemaList() const
    {
        std::vector<std::pair<NYT::TRichYPath, NYT::TTableSchema>> result;
        result.emplace_back(GetPath(), GetSchema());
        for (const auto& write : SecondaryYtWriteList_) {
            result.emplace_back(write->GetPath(), write->GetSchema());
        }
        return result;
    }

    void MergeFrom(const TOutputTableNode& other)
    {
        SecondaryYtWriteList_.push_back(std::move(other.RawYtWrite_));
        SecondaryYtWriteList_.insert(SecondaryYtWriteList_.end(), other.SecondaryYtWriteList_.begin(), other.SecondaryYtWriteList_.end());
    }

private:
    std::shared_ptr<TTableNode> Clone() const override
    {
        auto result = std::make_shared<TOutputTableNode>(Vtable, RawYtWrite_);
        result->SecondaryYtWriteList_ = SecondaryYtWriteList_;
        return result;
    }

private:
    const IRawYtWritePtr RawYtWrite_;
    std::vector<IRawYtWritePtr> SecondaryYtWriteList_;
};

////////////////////////////////////////////////////////////////////////////////

class TIntermediateTableNode
    : public TYtGraphV2::TTableNode
{
public:
    TIntermediateTableNode(TRowVtable vtable, TString temporaryDirectory, bool useProtoFormat)
        : TYtGraphV2::TTableNode(std::move(vtable))
        , TemporaryDirectory_(std::move(temporaryDirectory))
        , UseProtoFormat_(useProtoFormat)
    { }

    ETableType GetTableType() const override
    {
        return ETableType::Intermediate;
    }

    ETableFormat GetTableFormat() const override
    {
        return UseProtoFormat_ ? ETableFormat::Proto : ETableFormat::TNode;
    }

    IRawParDoPtr CreateDecodingParDo() const override
    {
        if (UseProtoFormat_) {
            return CreateDecodingValueProtoParDo(Vtable);
        } else {
            return CreateDecodingValueNodeParDo(Vtable);
        }
    }

    IRawParDoPtr CreateWriteParDo(ssize_t tableIndex) const override
    {
        if (UseProtoFormat_) {
            auto writeParDo = MakeIntrusive<TWriteProtoParDo<TKVProto>>();
            writeParDo->SetTableIndex(tableIndex);

            return writeParDo;
        } else {
            return CreateWriteNodeParDo(tableIndex);
        }
    }

    IRawParDoPtr CreateEncodingParDo() const override
    {
        if (UseProtoFormat_) {
            return CreateEncodingValueProtoParDo(Vtable);
        } else {
            return CreateEncodingValueNodeParDo(Vtable);
        }
    }

    virtual NYT::TRichYPath GetPath() const override
    {
        Y_ABORT_UNLESS(OutputOf.Operation);

        TStringStream tableName;
        tableName << OutputOf.Operation->GetFirstName();
        tableName << "." << std::visit([] (const auto& connector) {
            using TType = std::decay_t<decltype(connector)>;
            if constexpr (std::is_same_v<TMergeOutputConnector, TType>) {
                return ToString("Merge");
            } else if constexpr (std::is_same_v<TMapperOutputConnector, TType>) {
                return ToString("Map.") + connector.MapperIndex + "." + connector.NodeId;
            } else if constexpr (std::is_same_v<TReducerOutputConnector, TType>) {
                return ToString("Reduce.") + connector.NodeId;
            } else {
                static_assert(std::is_same_v<TMapperOutputConnector, TType>);
            }
        }, OutputOf.Connector);

        return TemporaryDirectory_ + "/" + tableName.Str();
    }

    const ::google::protobuf::Descriptor* GetProtoDescriptor() const override
    {
        return UseProtoFormat_ ? TKVProto::GetDescriptor() : nullptr;
    }

private:
    std::shared_ptr<TTableNode> Clone() const override
    {
        return std::make_shared<TIntermediateTableNode>(Vtable, TemporaryDirectory_, UseProtoFormat_);
    }

private:
    const TString TemporaryDirectory_;
    bool UseProtoFormat_;
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateReadImpulseParDo(const std::vector<TTableNode*>& inputTables)
{
    Y_ENSURE(!inputTables.empty(), "Expected 'inputTables' to be nonempty");

    std::vector<TRowVtable> vtables;
    auto format = inputTables[0]->GetTableFormat();
    for (const auto& table : inputTables) {
        Y_ENSURE(table->GetTableFormat() == format, "Format of input tables is different");

        if (format == ETableFormat::Proto) {
            vtables.emplace_back(table->Vtable);
        }
    }

    if (format == ETableFormat::TNode) {
        return CreateReadNodeImpulseParDo(std::ssize(inputTables));
    } else {
        return CreateReadProtoImpulseParDo(std::move(vtables));
    }
}

////////////////////////////////////////////////////////////////////////////////

NYT::TFormat GetInputFormat(const std::vector<TTableNode*>& inputTables)
{
    Y_ENSURE(!inputTables.empty(), "Unexpected number of input tables");

    TVector<const ::google::protobuf::Descriptor*> descriptors;
    auto format = inputTables[0]->GetTableFormat();
    for (const auto& table : inputTables) {
        Y_ENSURE(table->GetTableFormat() == format, "Format of input tables is different");

        if (format == ETableFormat::Proto) {
            descriptors.emplace_back(table->GetProtoDescriptor());
        }
    }

    switch (format) {
        case ETableFormat::TNode:
            return NYT::TFormat::YsonBinary();
        case ETableFormat::Proto:
            return NYT::TFormat::Protobuf(descriptors, true);
        default:
            Y_ABORT("Unsupported table format");
    }
}

NYT::TFormat GetOutputFormat(const THashMap<TOperationConnector, TTableNode*>& outputTables)
{
    Y_ENSURE(!outputTables.empty(), "Unexpected number of output tables");

    TVector<const ::google::protobuf::Descriptor*> descriptors;
    auto format = outputTables.begin()->second->GetTableFormat();
    for (const auto& [_, table] : outputTables) {
        Y_ENSURE(table->GetTableFormat() == format, "Format of output tables is different");

        if (format == ETableFormat::Proto) {
            descriptors.emplace_back(table->GetProtoDescriptor());
        }
    }

    switch (format) {
        case ETableFormat::TNode:
            return NYT::TFormat::YsonBinary();
        case ETableFormat::Proto:
            return NYT::TFormat::Protobuf(descriptors, true);
        default:
            Y_ABORT("Unsupported table format");
    }
}

NYT::TFormat GetIntermediatesFormat(bool useProtoFormat)
{
    if (!useProtoFormat) {
        return NYT::TFormat::YsonBinary();
    }

    TVector<const ::google::protobuf::Descriptor*> descriptors;
    descriptors.emplace_back(TKVProto::GetDescriptor());

    return NYT::TFormat::Protobuf(descriptors, true);
}

////////////////////////////////////////////////////////////////////////////////

class TYtGraphV2::TPlainGraph
{
public:
    std::vector<TTableNodePtr> Tables;
    std::vector<TOperationNodePtr> Operations;

public:
    TMergeOperationNode* CreateMergeOperation(
        std::vector<TTableNode*> inputs,
        const TString& firstName)
    {
        auto operation = TMergeOperationNode::MakeShared(firstName, {firstName});
        Operations.push_back(operation);
        LinkWithInputs(inputs, operation.get());
        return operation.get();
    }

    std::pair<TMapOperationNode*, std::vector<TParDoTreeBuilder::TPCollectionNodeId>> CreateMapOperation(
        std::vector<TTableNode*> inputs,
        TString firstName,
        const IRawParDoPtr& rawParDo)
    {
        auto operation = TMapOperationNode::MakeShared(firstName, THashSet<TString>{firstName});
        Operations.push_back(operation);
        LinkWithInputs(std::move(inputs), operation.get());
        auto outputIdList = operation->MapperBuilder_.AddParDo(rawParDo, TParDoTreeBuilder::RootNodeId);
        return {operation.get(), std::move(outputIdList)};
    }

    std::pair<TOperationNode*,TTableNode*> CreateIdentityMapOperation(
        std::vector<TTableNode*> inputs,
        TString firstName,
        TRowVtable outputVtable,
        IRawYtWritePtr rawYtWrite)
    {
        auto operation = TMapOperationNode::MakeShared(firstName, THashSet<TString>{firstName});
        Operations.push_back(operation);
        LinkWithInputs(std::move(inputs), operation.get());

        auto table = std::make_shared<TOutputTableNode>(
            std::move(outputVtable),
            rawYtWrite);
        Tables.push_back(table);
        table->OutputOf = {.Operation = operation.get(), .Connector = {}};
        operation->OutputTables[TMapperOutputConnector{}] = table.get();

        return {operation.get(), table.get()};
    }

    TMapReduceOperationNode* CreateMapReduceOperation(std::vector<TTableNode*> inputs, const TString& firstName)
    {
        auto operation = TMapReduceOperationNode::MakeShared(firstName, THashSet<TString>{firstName}, std::ssize(inputs));
        Operations.push_back(operation);
        LinkWithInputs(std::move(inputs), operation.get());
        return operation.get();
    }

    std::pair<TMapReduceOperationNode*, TParDoTreeBuilder::TPCollectionNodeId> CreateGroupByKeyMapReduceOperation(
        TTableNode* inputTable,
        TString firstName,
        const IRawGroupByKeyPtr& rawGroupByKey,
        bool useProtoFormat)
    {
        auto* mapReduceOperation = CreateMapReduceOperation({inputTable}, firstName);

        auto parDoList = useProtoFormat
            ? std::vector<IRawParDoPtr>{
                CreateEncodingKeyValueProtoParDo(inputTable->Vtable),
                CreateWriteProtoParDo<TKVProto>(0)
            }
            : std::vector<IRawParDoPtr>{
                CreateEncodingKeyValueNodeParDo(inputTable->Vtable),
                CreateWriteNodeParDo(0)
            };

        mapReduceOperation->MapperBuilderList_[0].AddParDoChainVerifyNoOutput(
            TParDoTreeBuilder::RootNodeId,
            parDoList
        );

        auto nodeId = mapReduceOperation->ReducerBuilder_.AddParDoVerifySingleOutput(
            useProtoFormat
                ? CreateGbkImpulseReadProtoParDo(rawGroupByKey)
                : CreateGbkImpulseReadNodeParDo(rawGroupByKey),
            TParDoTreeBuilder::RootNodeId
        );
        return {mapReduceOperation, nodeId};
    }

    std::pair<TMapReduceOperationNode*, TParDoTreeBuilder::TPCollectionNodeId> CreateCoGroupByKeyMapReduceOperation(
        std::vector<TTableNode*> inputTableList,
        TString firstName,
        const IRawCoGroupByKeyPtr& rawCoGroupByKey,
        bool useProtoFormat)
    {
        auto* mapReduceOperation = CreateMapReduceOperation(inputTableList, firstName);
        Y_ABORT_UNLESS(std::ssize(mapReduceOperation->GetMapperBuilderList()) == std::ssize(inputTableList));
        for (ssize_t i = 0; i < std::ssize(inputTableList); ++i) {
            auto* inputTable = inputTableList[i];

            auto parDoList = useProtoFormat
                ? std::vector<IRawParDoPtr>{
                    CreateEncodingKeyValueProtoParDo(inputTable->Vtable),
                    MakeRawParDo(::MakeIntrusive<TAddTableIndexToProtoDoFn>(i)),
                    CreateWriteProtoParDo<TKVProto>(0)
                }
                : std::vector<IRawParDoPtr>{
                    CreateEncodingKeyValueNodeParDo(inputTable->Vtable),
                    MakeRawParDo(::MakeIntrusive<TAddTableIndexDoFn>(i)),
                    CreateWriteNodeParDo(0)
                };

            mapReduceOperation->MapperBuilderList_[i].AddParDoChainVerifyNoOutput(
                TParDoTreeBuilder::RootNodeId,
                parDoList
            );
        }

        std::vector<TRowVtable> inputRowVtableList;
        for (const auto& inputTable : inputTableList) {
            inputRowVtableList.push_back(inputTable->Vtable);
        }

        auto nodeId = mapReduceOperation->ReducerBuilder_.AddParDoVerifySingleOutput(
            useProtoFormat
                ? CreateCoGbkImpulseReadProtoParDo(
                    rawCoGroupByKey,
                    inputRowVtableList
                )
                : CreateCoGbkImpulseReadNodeParDo(
                    rawCoGroupByKey,
                    inputRowVtableList
                ),
            TParDoTreeBuilder::RootNodeId
        );
        return {mapReduceOperation, nodeId};
    }

    std::pair<TMapReduceOperationNode*, TParDoTreeBuilder::TPCollectionNodeId> CreateCombinePerKeyMapReduceOperation(
        TTableNode* inputTable,
        TString firstName,
        const IRawCombinePtr& rawCombine)
    {
        auto* mapReduceOperation = CreateMapReduceOperation({inputTable}, firstName);
        mapReduceOperation->MapperBuilderList_[0].AddParDoChainVerifyNoOutput(
            TParDoTreeBuilder::RootNodeId,
            {
                CreateEncodingKeyValueNodeParDo(inputTable->Vtable),
                CreateWriteNodeParDo(0)
            }
        );

        mapReduceOperation->CombinerBuilder_.AddParDoChainVerifyNoOutput(
            TParDoTreeBuilder::RootNodeId,
            {
                CreateCombineCombinerImpulseReadParDo(rawCombine),
            }
        );

        auto nodeId = mapReduceOperation->ReducerBuilder_.AddParDoChainVerifySingleOutput(
            TParDoTreeBuilder::RootNodeId,
            {
                CreateCombineReducerImpulseReadParDo(rawCombine),
            }
        );

        return {mapReduceOperation, nodeId};
    }

    TInputTableNode* CreateInputTable(TRowVtable vtable, const IRawYtReadPtr& rawYtRead)
    {
        auto table = std::make_shared<TInputTableNode>(std::move(vtable), rawYtRead);
        Tables.push_back(table);
        return table.get();
    }

    TOutputTableNode* CreateOutputTable(
        TOperationNode* operation,
        TOperationConnector connector,
        TRowVtable vtable,
        IRawYtWritePtr rawYtWrite)
    {
        auto table = std::make_shared<TOutputTableNode>(std::move(vtable), rawYtWrite);
        Tables.push_back(table);
        LinkOperationOutput(operation, connector, table.get());
        return table.get();
    }

    TIntermediateTableNode* CreateIntermediateTable(
        TOperationNode* operation,
        TOperationConnector connector,
        TRowVtable vtable,
        TString temporaryDirectory,
        bool useProtoFormat)
    {
        auto table = std::make_shared<TIntermediateTableNode>(std::move(vtable), std::move(temporaryDirectory), useProtoFormat);
        Tables.push_back(table);
        LinkOperationOutput(operation, connector, table.get());
        return table.get();
    }

    // Метод предназначен, для того чтобы клонировать вершину с операцией в новый граф.
    // Клонированная вершина выражает ту же самую пользовательскую логику, но оперирует на входных таблицах нового графа.
    // Этот метод не клонирует выходные таблицы, подразумевается, что пользователь сделает это сам, возможно с какими-то модификациями.
    TOperationNode* CloneOperation(std::vector<TTableNode*> clonedInputs, const TOperationNode* originalOperation)
    {
        Y_ABORT_UNLESS(clonedInputs.size() == originalOperation->InputTables.size());
        auto cloned = originalOperation->Clone();
        Operations.push_back(cloned);
        LinkWithInputs(std::move(clonedInputs), cloned.get());
        return cloned.get();
    }

    TTableNode* CloneTable(TOperationNode* clonedOperation, TOperationConnector clonedConnector, const TTableNode* originalTable)
    {
        auto clonedTable = originalTable->Clone();
        Tables.push_back(clonedTable);
        if (clonedOperation) {
            auto inserted = clonedOperation->OutputTables.emplace(clonedConnector, clonedTable.get()).second;
            Y_ABORT_UNLESS(inserted);
            clonedTable->OutputOf = {.Operation = clonedOperation, .Connector = clonedConnector};
        }
        return clonedTable.get();
    }

    static void LinkOperationOutput(TOperationNode* operation, TOperationConnector connector, TTableNode* table)
    {
        Y_ABORT_UNLESS(table->OutputOf.Operation == nullptr);
        auto inserted = operation->OutputTables.emplace(connector, table).second;
        Y_ABORT_UNLESS(inserted);
        table->OutputOf = {.Operation = operation, .Connector = connector};
    }

    static TTableNode* UnlinkOperationOutput(TOperationNode* operation, TOperationConnector connector)
    {
        auto it = operation->OutputTables.find(connector);
        Y_ABORT_UNLESS(it != operation->OutputTables.end());

        auto* table = it->second;
        table->OutputOf = {};

        operation->OutputTables.erase(it);
        return table;
    }

    static void ResetOperationInput(TOperationNode* operation, ssize_t inputIndex)
    {
        Y_ABORT_UNLESS(inputIndex >= 0);
        Y_ABORT_UNLESS(inputIndex < std::ssize(operation->InputTables));

        auto inputTable = operation->InputTables[inputIndex];

        // TODO: нужно заменить на set
        auto it = std::find(
            inputTable->InputFor.begin(),
            inputTable->InputFor.end(),
            TTableNode::TInputFor{operation, inputIndex}
        );
        Y_ABORT_UNLESS(it != inputTable->InputFor.end());
        inputTable->InputFor.erase(it);
        operation->InputTables[inputIndex] = nullptr;
    }

    static void ReplaceOperationInput(TOperationNode* operation, ssize_t inputIndex, TTableNode* newInputTable)
    {
        ResetOperationInput(operation, inputIndex);

        operation->InputTables[inputIndex] = newInputTable;
        newInputTable->InputFor.push_back(TTableNode::TInputFor{operation, inputIndex});
    }

    static void RelinkTableConsumers(TTableNode* oldTable, TTableNode* newTable)
    {
        for (const auto& inputFor : oldTable->InputFor) {
            auto* consumerOperation = inputFor.Operation;
            Y_ABORT_UNLESS(std::ssize(consumerOperation->InputTables) > inputFor.Index);
            Y_ABORT_UNLESS(consumerOperation->InputTables[inputFor.Index] == oldTable);

            consumerOperation->InputTables[inputFor.Index] = newTable;
            newTable->InputFor.push_back(inputFor);
        }
        oldTable->InputFor.clear();
    }

private:
    void LinkWithInputs(std::vector<TTableNode*> inputs, TOperationNode* operation)
    {
        operation->InputTables = std::move(inputs);
        for (ssize_t index = 0; index < std::ssize(operation->InputTables); ++index) {
            auto* table = operation->InputTables[index];
            table->InputFor.push_back({
                .Operation = operation,
                .Index = index,
            });
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

THashMap<TParDoTreeBuilder::TPCollectionNodeId, TParDoTreeBuilder::TPCollectionNodeId>
TMapReduceOperationNode::FusePrecedingMapper(ssize_t mapperIndex)
{
    Y_ABORT_UNLESS(mapperIndex < std::ssize(MapperBuilderList_));

    auto mapOperation = InputTables[mapperIndex]->OutputOf.Operation->VerifiedAsPtr<TMapOperationNode>();
    auto mapOperationConnector = InputTables[mapperIndex]->OutputOf.Connector;

    MergeTransformNames(mapOperation->TransformNames_);

    auto fusedBuilder = mapOperation->GetMapperBuilder();

    auto result = fusedBuilder.Fuse(MapperBuilderList_[mapperIndex], VerifiedGetNodeIdOfMapperConnector(mapOperationConnector));
    MapperBuilderList_[mapperIndex] = fusedBuilder;

    // Going to modify mapOperation connectors.
    // Don't want to modify hash map while traversing it,
    // so we collect connectors to modify into a vector.
    std::vector<TOperationConnector> connectorsToRelink;
    auto mapOperationOutputTablesCopy = mapOperation->OutputTables;
    for (const auto& [connector, outputTable] : mapOperationOutputTablesCopy) {
        if (outputTable->GetTableType() == ETableType::Output) {
            connectorsToRelink.push_back(connector);
        }
        // N.B. We don't want to relink intermediate tables.
        // They are connected to other MapReduce operations.
        // If such intermediate tables (and corresponding consuming MapReduce operations) are present,
        // this map operation will be also fused into other MapReduce operation.
    }
    for (const auto& connector : connectorsToRelink) {
        auto nodeId = VerifiedGetNodeIdOfMapperConnector(connector);
        auto outputTable = TYtGraphV2::TPlainGraph::UnlinkOperationOutput(mapOperation, connector);
        auto newConnector = TMapperOutputConnector{mapperIndex, nodeId};
        TYtGraphV2::TPlainGraph::LinkOperationOutput(this, newConnector, outputTable);
    }
    TYtGraphV2::TPlainGraph::ReplaceOperationInput(
        this,
        mapperIndex,
        mapOperation->VerifiedGetSingleInput()
    );
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class IPlainGraphVisitor
{
public:
    virtual ~IPlainGraphVisitor() = default;

    virtual void OnTableNode(TYtGraphV2::TTableNode* /*tableNode*/)
    { }

    virtual void OnOperationNode(TYtGraphV2::TOperationNode* /*operationNode*/)
    { }
};

////////////////////////////////////////////////////////////////////////////////

void TraverseInTopologicalOrder(const TYtGraphV2::TPlainGraph& plainGraph, IPlainGraphVisitor* visitor)
{
    THashSet<TYtGraphV2::TTableNode*> visited;
    auto tryVisitTable = [&] (TYtGraphV2::TTableNode* table) {
        auto inserted = visited.insert(table).second;
        if (inserted) {
            visitor->OnTableNode(table);
        }
    };

    for (const auto& operation : plainGraph.Operations) {
        for (const auto& table : operation->InputTables) {
            tryVisitTable(table);
        }
        visitor->OnOperationNode(operation.get());
        for (const auto& [connector, table] : operation->OutputTables) {
            tryVisitTable(table);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

/// TEphemeralImage is an image that is inside some TParDoTree builder.
struct TEphemeralImage
{
    TYtGraphV2::TOperationNode* Operation;
    TOperationConnector Connector;

    TEphemeralImage(TYtGraphV2::TOperationNode* operation, const TOperationConnector&& connector)
        : Operation(operation)
        , Connector(connector)
    { }
};

////////////////////////////////////////////////////////////////////////////////o

class TTableProjection
{
public:
    TYtGraphV2::TTableNode* VerifiedGetClonedTable(const TYtGraphV2::TTableNode* oldTable)
    {
        auto it = TableMap_.find(oldTable);
        Y_ABORT_UNLESS(it != TableMap_.end());
        return it->second;
    }

    const THashSet<const TTableNode*> VerifiedGetOriginalTableSet(TYtGraphV2::TTableNode* newTable)
    {
        auto it = ReverseTableMap_.find(newTable);
        Y_ABORT_UNLESS(it != ReverseTableMap_.end());
        return it->second;
    }

    TOperationConnector ResolveNewConnection(const TTableNode* oldTableNode)
    {
        Y_ABORT_UNLESS(oldTableNode->OutputOf.Operation);
        Y_ABORT_UNLESS(oldTableNode->OutputOf.Operation->GetOperationType() == EOperationType::Map);

        auto it = TableMap_.find(oldTableNode);
        Y_ABORT_UNLESS(it != TableMap_.end());

        auto* newTableNode = it->second;
        return newTableNode->OutputOf.Connector;
    }

    void RegisterTableProjection(const TTableNode* original, TTableNode* cloned)
    {
        bool inserted;
        inserted = TableMap_.emplace(original, cloned).second;
        Y_ABORT_UNLESS(inserted);
        inserted = ReverseTableMap_[cloned].insert(original).second;
        Y_ABORT_UNLESS(inserted);
    }

    void DeregisterTableProjection(const TTableNode* original)
    {
        auto itTableMap = TableMap_.find(original);
        Y_ABORT_UNLESS(itTableMap != TableMap_.end());
        auto* cloned  = itTableMap->second;
        TableMap_.erase(itTableMap);

        auto itReverseTableMap = ReverseTableMap_.find(cloned);
        Y_ABORT_UNLESS(itReverseTableMap != ReverseTableMap_.end());
        auto& originalSet = itReverseTableMap->second;

        auto itOriginalSet = originalSet.find(original);
        Y_ABORT_UNLESS(itOriginalSet != originalSet.end());

        originalSet.erase(itOriginalSet);

        if (originalSet.empty()) {
            ReverseTableMap_.erase(itReverseTableMap);
        }
    }

    void ProjectTable(
        TYtGraphV2::TPlainGraph* plainGraph,
        TOperationNode* clonedOperation,
        TOperationConnector clonedConnector,
        const TTableNode* originalTable)
    {
        TTableNode* newClonedTable = nullptr;
        TTableNode* oldClonedTable = nullptr;
        if (clonedOperation) {
            if (auto it = clonedOperation->OutputTables.find(clonedConnector); it != clonedOperation->OutputTables.end()) {
                oldClonedTable = it->second;
            }
        }

        if (oldClonedTable == nullptr) {
            newClonedTable = plainGraph->CloneTable(clonedOperation, clonedConnector, originalTable);
            RegisterTableProjection(originalTable, newClonedTable);
        } else if (oldClonedTable->GetTableType() == ETableType::Intermediate) {
            Y_ABORT_UNLESS(originalTable->GetTableType() == ETableType::Output);

            auto unlinked = plainGraph->UnlinkOperationOutput(clonedOperation, clonedConnector);
            Y_ABORT_UNLESS(unlinked == oldClonedTable);

            newClonedTable = plainGraph->CloneTable(clonedOperation, clonedConnector, originalTable);
            RegisterTableProjection(originalTable, newClonedTable);

            plainGraph->RelinkTableConsumers(oldClonedTable, newClonedTable);

            auto oldOriginalList = VerifiedGetOriginalTableSet(oldClonedTable);
            for (auto* original : oldOriginalList) {
                DeregisterTableProjection(original);
                RegisterTableProjection(original, newClonedTable);
            }
        } else if (oldClonedTable->GetTableType() == ETableType::Output && originalTable->GetTableType() == ETableType::Intermediate) {
            RegisterTableProjection(originalTable, oldClonedTable);
        } else {
            Y_ABORT_UNLESS(oldClonedTable->GetTableType() == ETableType::Output);
            Y_ABORT_UNLESS(originalTable->GetTableType() == ETableType::Output);

            oldClonedTable->VerifiedAsPtr<TOutputTableNode>()->MergeFrom(
                *originalTable->VerifiedAsPtr<TOutputTableNode>()
            );
            RegisterTableProjection(originalTable, oldClonedTable);
        }
    }

private:
    THashMap<const TYtGraphV2::TTableNode*, TYtGraphV2::TTableNode*> TableMap_;
    THashMap<TTableNode*, THashSet<const TTableNode*>> ReverseTableMap_;
};

////////////////////////////////////////////////////////////////////////////////

class IYtGraphOptimizer
{
public:
    virtual ~IYtGraphOptimizer() = default;

    virtual TYtGraphV2::TPlainGraph Optimize(const TYtGraphV2::TPlainGraph& plainGraph) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TMapFuser
    : public IYtGraphOptimizer
{
public:
    virtual TYtGraphV2::TPlainGraph Optimize(const TYtGraphV2::TPlainGraph& plainGraph) override
    {
        TVisitor visitor;
        TraverseInTopologicalOrder(plainGraph, &visitor);
        return visitor.ExtractOutput();
    }

private:
    class TVisitor
        : public IPlainGraphVisitor
    {
    public:
        void OnTableNode(TTableNode* originalTable) override
        {
            if (originalTable->GetTableType() == ETableType::Input) {
                TableProjection_.ProjectTable(&PlainGraph_, nullptr, {}, originalTable);
            }
            // All non input tables are cloned during clonning of their producing operations.
        }

        void OnOperationNode(TYtGraphV2::TOperationNode* originalOperation) override
        {
            auto matchProducerConsumerPattern = [] (const TOperationNode* operationNode) -> TOperationNode* {
                if (operationNode->GetOperationType() == EOperationType::Map) {
                    auto originalInput = operationNode->VerifiedGetSingleInput();
                    auto originalPreviousOperation = originalInput->OutputOf.Operation;
                    if (originalPreviousOperation && originalPreviousOperation->GetOperationType() == EOperationType::Map) {
                        return originalPreviousOperation;
                    }
                }
                return nullptr;
            };

            if (FusionMap_.contains(originalOperation)) {
                return;
            } else if (auto originalPreviousOperation = matchProducerConsumerPattern(originalOperation)) {
                // Okay we have chained sequence of maps here.
                // Previous map must be already fused.
                auto clonedFusionDestination = FusionMap_[originalPreviousOperation];
                Y_ABORT_UNLESS(clonedFusionDestination);

                auto connectTo = TableProjection_.ResolveNewConnection(originalOperation->VerifiedGetSingleInput());
                FuseMapTo(originalOperation, clonedFusionDestination, VerifiedGetNodeIdOfMapperConnector(connectTo));
            } else {
                std::vector<TYtGraphV2::TTableNode*> clonedInputs;
                for (const auto* originalInputTable : originalOperation->InputTables) {
                    auto clonedInputTable = TableProjection_.VerifiedGetClonedTable(originalInputTable);
                    clonedInputs.push_back(clonedInputTable);
                }

                auto* clonedOperation = PlainGraph_.CloneOperation(clonedInputs, originalOperation);

                for (const auto& [originalConnector, originalOutputTable] : originalOperation->OutputTables) {
                    auto clonedConnector = originalConnector;
                    TableProjection_.ProjectTable(&PlainGraph_, clonedOperation, clonedConnector, originalOutputTable);
                }

                if (originalOperation->GetOperationType() == EOperationType::Map) {
                    auto inserted = FusionMap_.emplace(originalOperation, clonedOperation).second;
                    Y_ABORT_UNLESS(inserted);

                    Y_ABORT_UNLESS(std::ssize(clonedOperation->OutputTables) == std::ssize(originalOperation->OutputTables));
                    // 1. Fuse all siblings.
                    const auto* originalInput = originalOperation->VerifiedGetSingleInput();
                    for (const auto& inputFor : originalInput->InputFor) {
                        const auto* originalSibling = inputFor.Operation;
                        if (originalSibling != originalOperation && originalSibling->GetOperationType() == EOperationType::Map) {
                            FuseMapTo(originalSibling, clonedOperation, TParDoTreeBuilder::RootNodeId);
                        }
                    }
                }
            }
        }

        void FuseMapTo(const TOperationNode* originalOperation, TOperationNode* destination, TParDoTreeBuilder::TPCollectionNodeId fusionPoint)
        {
            bool inserted = false;

            const auto* originalMap = originalOperation->VerifiedAsPtr<TMapOperationNode>();
            auto* destinationMap = destination->VerifiedAsPtr<TMapOperationNode>();

            auto originalToDestinationConnectorMap = destinationMap->Fuse(*originalMap, fusionPoint);
            inserted = FusionMap_.emplace(originalOperation, destination).second;
            Y_ABORT_UNLESS(inserted);

            for (const auto& [originalConnector, originalOutputTable] : originalMap->OutputTables) {
                auto connector = originalToDestinationConnectorMap[VerifiedGetNodeIdOfMapperConnector(originalConnector)];

                TableProjection_.ProjectTable(&PlainGraph_, destination, TMapperOutputConnector{0, connector}, originalOutputTable);
            }
        }

        TYtGraphV2::TPlainGraph ExtractOutput()
        {
            auto result = std::move(PlainGraph_);
            *this = {};
            return result;
        }

    private:
        // Original map operation -> cloned map operation, that original operation is fused into
        // Trivial fusion is also counts, i.e. if fusion contains only one original map operation.
        THashMap<const TOperationNode*, TOperationNode*> FusionMap_;

        TTableProjection TableProjection_;

        TYtGraphV2::TPlainGraph PlainGraph_;
    };
};

////////////////////////////////////////////////////////////////////////////////

class TMapReduceFuser
    : public IYtGraphOptimizer
{
public:
    TYtGraphV2::TPlainGraph Optimize(const TYtGraphV2::TPlainGraph& plainGraph) override
    {
        TVisitor visitor;
        TraverseInTopologicalOrder(plainGraph, &visitor);
        auto result = visitor.ExtractOutput();

        RemoveHangingMapOperations(&result);

        return result;
    }

private:
    static void RemoveHangingMapOperations(TYtGraphV2::TPlainGraph* plainGraph)
    {
        while (true) {
            std::vector<TTableNodePtr> newTables;
            for (const auto& table : plainGraph->Tables) {
                if (table->GetTableType() == ETableType::Intermediate && table->InputFor.empty()) {
                    plainGraph->UnlinkOperationOutput(table->OutputOf.Operation, table->OutputOf.Connector);
                } else {
                    newTables.push_back(table);
                }
            }
            if (newTables.size() == plainGraph->Tables.size()) {
                break;
            }
            plainGraph->Tables = std::move(newTables);

            std::vector<TOperationNodePtr> newOperations;
            for (const auto& operation : plainGraph->Operations) {
                if (operation->OutputTables.empty()) {
                    Y_ABORT_UNLESS(operation->GetOperationType() == EOperationType::Map);
                    Y_ABORT_UNLESS(operation->InputTables.size() == 1);
                    plainGraph->ResetOperationInput(operation.get(), 0);
                } else {
                    newOperations.push_back(operation);
                }
            }
            if (newOperations.size() == plainGraph->Operations.size()) {
                break;
            }
            plainGraph->Operations = std::move(newOperations);
        }
    }

    class TVisitor
        : public IPlainGraphVisitor
    {
    public:
        void OnTableNode(TTableNode* originalTable) override
        {
            if (originalTable->GetTableType() == ETableType::Input) {
                TableProjection_.ProjectTable(&PlainGraph_, nullptr, {}, originalTable);
            }
        }

        void OnOperationNode(TYtGraphV2::TOperationNode* originalOperation) override
        {
            switch (originalOperation->GetOperationType()) {
                case EOperationType::Map: {
                    const auto* originalInput = originalOperation->VerifiedGetSingleInput();
                    auto* clonedInput = TableProjection_.VerifiedGetClonedTable(originalInput);
                    if (clonedInput->OutputOf.Operation && clonedInput->OutputOf.Operation->GetOperationType() != EOperationType::Merge) {
                        switch (clonedInput->OutputOf.Operation->GetOperationType()) {
                            case EOperationType::Merge: {
                                // We have just checked that this operation is not merge.
                                Y_ABORT("Unexpected Merge operation");
                            }
                            case EOperationType::Map:
                                // It should not be EOperationType::Map operation
                                // since EOperationType::Map operation would be fused by previous stage Fuse maps
                                Y_ABORT("Unexpected Map operation");
                            case EOperationType::MapReduce: {
                                // It should not be EOperationType::Map operation
                                // since EOperationType::Map operation would be fused by previous stage Fuse maps
                                auto* clonedMapReduceOperation = clonedInput->OutputOf.Operation->VerifiedAsPtr<TMapReduceOperationNode>();
                                auto [clonedReducerBuilder, clonedReducerConnectorNodeId] = clonedMapReduceOperation->ResolveOperationConnector(clonedInput->OutputOf.Connector);
                                auto originalMapOperation = originalOperation->VerifiedAsPtr<TMapOperationNode>();
                                auto fusedConnectorMap = clonedMapReduceOperation->FuseToReducer(*originalMapOperation, clonedReducerConnectorNodeId);
                                for (const auto& [originalConnector, originalOutputTable] : originalOperation->OutputTables) {
                                    auto originalNodeId = VerifiedGetNodeIdOfMapperConnector(originalConnector);
                                    auto it = fusedConnectorMap.find(originalNodeId);
                                    Y_ABORT_UNLESS(it != fusedConnectorMap.end());
                                    auto clonedNodeId = it->second;
                                    TableProjection_.ProjectTable(
                                        &PlainGraph_,
                                        clonedMapReduceOperation,
                                        TReducerOutputConnector{clonedNodeId},
                                        originalOutputTable
                                    );
                                }
                                return;
                            }
                        }
                        Y_ABORT("Unexpected type of operation");
                    } else {
                        auto clonedOperation = PlainGraph_.CloneOperation({clonedInput}, originalOperation);
                        for (const auto& [originalConnector, originalOutputTable] : originalOperation->OutputTables) {
                            TableProjection_.ProjectTable(&PlainGraph_, clonedOperation, originalConnector, originalOutputTable);
                        }
                        return;
                    }
                }
                case EOperationType::MapReduce: {
                    std::vector<TTableNode*> clonedInputList;
                    for (const auto* originalInput : originalOperation->InputTables) {
                        auto* clonedInput = TableProjection_.VerifiedGetClonedTable(originalInput);
                        clonedInputList.push_back(clonedInput);
                    }
                    auto clonedOperation = PlainGraph_.CloneOperation({clonedInputList}, originalOperation)->VerifiedAsPtr<TMapReduceOperationNode>();
                    THashMap<std::pair<ssize_t, TParDoTreeBuilder::TPCollectionNodeId>, TParDoTreeBuilder::TPCollectionNodeId> mapperConnectorMap;

                    Y_ABORT_UNLESS(std::ssize(clonedOperation->InputTables) == std::ssize(clonedInputList));

                    // Fuse mapper stage with previous maps if possible
                    for (ssize_t i = 0; i < std::ssize(clonedOperation->InputTables); ++i) {
                        const auto& clonedInputTable = clonedOperation->InputTables[i];
                        if (clonedInputTable->OutputOf.Operation
                            && clonedInputTable->OutputOf.Operation->GetOperationType() == EOperationType::Map)
                        {
                            auto idMap = clonedOperation->FusePrecedingMapper(i);
                            for (const auto& item : idMap) {
                                auto inserted = mapperConnectorMap.emplace(std::pair{i, item.first}, item.second).second;
                                Y_ABORT_UNLESS(inserted);
                            }
                        }
                    }

                    auto resolveClonedConnector = [&mapperConnectorMap] (const TOperationConnector& connector) {
                        return std::visit([&mapperConnectorMap] (const auto& connector) -> TOperationConnector {
                            using TType = std::decay_t<decltype(connector)>;
                            if constexpr (std::is_same_v<TType, TMergeOutputConnector>) {
                                return TMergeOutputConnector{};
                            } else if constexpr (std::is_same_v<TType, TMapperOutputConnector>) {
                                auto it = mapperConnectorMap.find(std::pair{connector.MapperIndex, connector.NodeId});
                                if (it == mapperConnectorMap.end()) {
                                    return connector;
                                } else {
                                    return TMapperOutputConnector{connector.MapperIndex, it->second};
                                }
                            } else if constexpr (std::is_same_v<TType, TReducerOutputConnector>) {
                                return connector;
                            } else {
                                static_assert(std::is_same_v<TType, void>);
                            }
                        }, connector);
                    };

                    for (const auto& [originalConnector, originalOutputTable] : originalOperation->OutputTables) {
                        auto clonedConnector = resolveClonedConnector(originalConnector);
                        TableProjection_.ProjectTable(&PlainGraph_, clonedOperation, clonedConnector, originalOutputTable);
                    }

                    return;
                }
                case EOperationType::Merge: {
                    std::vector<TYtGraphV2::TTableNode*> clonedInputs;
                    for (const auto* originalInputTable : originalOperation->InputTables) {
                        auto clonedInputTable = TableProjection_.VerifiedGetClonedTable(originalInputTable);
                        clonedInputs.push_back(clonedInputTable);
                    }

                    auto* clonedOperation = PlainGraph_.CloneOperation(clonedInputs, originalOperation);

                    for (const auto& [originalConnector, originalOutputTable] : originalOperation->OutputTables) {
                        auto clonedConnector = originalConnector;
                        TableProjection_.ProjectTable(&PlainGraph_, clonedOperation, clonedConnector, originalOutputTable);
                    }

                    return;
                }
            }
            Y_ABORT("unknown operation type");
        }

        TYtGraphV2::TPlainGraph ExtractOutput()
        {
            auto result = std::move(PlainGraph_);
            *this = {};
            return result;
        }

    private:
        TTableProjection TableProjection_;
        TYtGraphV2::TPlainGraph PlainGraph_;
    };
};

////////////////////////////////////////////////////////////////////////////////

class TYtGraphV2Builder
    : public IRawPipelineVisitor
{
public:
    TYtGraphV2Builder(const TYtPipelineConfig& config)
        : Config_(std::make_shared<TYtPipelineConfig>(config))
    { }

    void OnTransform(TTransformNode* transformNode) override
    {
        auto rawTransform = transformNode->GetRawTransform();
        switch (rawTransform->GetType()) {
            case ERawTransformType::Read:
                if (auto* rawYtRead = dynamic_cast<IRawYtRead*>(&*rawTransform->AsRawRead())) {
                    Y_ABORT_UNLESS(transformNode->GetSinkCount() == 1);
                    const auto* pCollectionNode = transformNode->GetSink(0).Get();
                    auto* tableNode = PlainGraph_->CreateInputTable(pCollectionNode->GetRowVtable(), IRawYtReadPtr{rawYtRead});
                    RegisterPCollection(pCollectionNode, tableNode);
                } else {
                    ythrow yexception() << transformNode->GetName() << " is not a YtRead and not supported";
                }
                break;
            case ERawTransformType::Write:
                if (auto* rawYtWrite = dynamic_cast<IRawYtWrite*>(&*rawTransform->AsRawWrite())) {
                    Y_ABORT_UNLESS(transformNode->GetSourceCount() == 1);
                    const auto* sourcePCollection = transformNode->GetSource(0).Get();
                    auto inputTable = GetPCollectionImage(sourcePCollection);
                    PlainGraph_->CreateIdentityMapOperation(
                        {inputTable},
                        transformNode->GetName(),
                        sourcePCollection->GetRowVtable(),
                        rawYtWrite
                    );
                } else {
                    Y_ABORT("YT executor doesn't support writes except YtWrite");
                }
                break;
            case ERawTransformType::ParDo: {
                const auto* sourcePCollection = VerifiedGetSingleSource(transformNode);
                auto* inputTable = GetPCollectionImage(sourcePCollection);
                auto rawParDo = rawTransform->AsRawParDo();

                Y_ABORT_UNLESS(!transformNode->GetName().empty());
                const auto& [mapOperation, outputIdList] = PlainGraph_->CreateMapOperation(
                    {inputTable},
                    transformNode->GetName(),
                    rawParDo
                );

                Y_ABORT_UNLESS(std::ssize(outputIdList) == transformNode->GetSinkCount());

                for (ssize_t i = 0; i < std::ssize(outputIdList); ++i) {
                    auto id = outputIdList[i];
                    const auto* pCollection = transformNode->GetSink(i).Get();
                    auto* outputTable = PlainGraph_->CreateIntermediateTable(
                        mapOperation,
                        TMapperOutputConnector{0, id},
                        pCollection->GetRowVtable(),
                        Config_->GetWorkingDir(),
                        Config_->GetEnableProtoFormatForIntermediates()
                    );
                    RegisterPCollection(pCollection, outputTable);
                }
                break;
            }
            case ERawTransformType::GroupByKey: {
                const auto* sourcePCollection = VerifiedGetSingleSource(transformNode);
                auto* inputTable = GetPCollectionImage(sourcePCollection);

                const auto& [mapReduceOperation, nodeId] = PlainGraph_->CreateGroupByKeyMapReduceOperation(
                    inputTable,
                    transformNode->GetName(),
                    transformNode->GetRawTransform()->AsRawGroupByKey(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );

                const auto* sinkPCollection = VerifiedGetSingleSink(transformNode);
                auto* outputTable = PlainGraph_->CreateIntermediateTable(
                    mapReduceOperation,
                    TReducerOutputConnector{nodeId},
                    sinkPCollection->GetRowVtable(),
                    Config_->GetWorkingDir(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );
                RegisterPCollection(sinkPCollection, outputTable);
                break;
            }
            case ERawTransformType::CoGroupByKey: {
                std::vector<TTableNode*> inputTableList;
                for (const auto& sourcePCollection : transformNode->GetSourceList()) {
                    auto* inputTable = GetPCollectionImage(sourcePCollection.Get());
                    inputTableList.push_back(inputTable);
                }

                const auto& [mapReduceOperation, nodeId] = PlainGraph_->CreateCoGroupByKeyMapReduceOperation(
                    inputTableList,
                    transformNode->GetName(),
                    transformNode->GetRawTransform()->AsRawCoGroupByKey(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );

                const auto* sinkPCollection = VerifiedGetSingleSink(transformNode);
                auto* outputTable = PlainGraph_->CreateIntermediateTable(
                    mapReduceOperation,
                    TReducerOutputConnector{nodeId},
                    sinkPCollection->GetRowVtable(),
                    Config_->GetWorkingDir(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );
                RegisterPCollection(sinkPCollection, outputTable);
                break;
            }
            case ERawTransformType::CombinePerKey: {
                // TODO support proto read/write
                const auto* sourcePCollection = VerifiedGetSingleSource(transformNode);
                auto* inputTable = GetPCollectionImage(sourcePCollection);
                auto rawCombinePerKey = transformNode->GetRawTransform()->AsRawCombine();

                const auto& [mapReduceOperation, nodeId] = PlainGraph_->CreateCombinePerKeyMapReduceOperation(
                    inputTable,
                    transformNode->GetName(),
                    transformNode->GetRawTransform()->AsRawCombine()
                );

                const auto* sinkPCollection = VerifiedGetSingleSink(transformNode);
                auto* outputTable = PlainGraph_->CreateIntermediateTable(
                    mapReduceOperation,
                    TReducerOutputConnector{nodeId},
                    sinkPCollection->GetRowVtable(),
                    Config_->GetWorkingDir(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );
                RegisterPCollection(sinkPCollection, outputTable);
                break;
            }
            case ERawTransformType::Flatten: {
                std::vector<TTableNode*> inputTableList;
                for (const auto& sourcePCollection : transformNode->GetSourceList()) {
                    auto* inputTable = GetPCollectionImage(sourcePCollection.Get());
                    inputTableList.push_back(inputTable);
                }

                Y_ABORT_UNLESS(!transformNode->GetName().Empty());
                auto* mergeOperation = PlainGraph_->CreateMergeOperation(
                    inputTableList,
                    transformNode->GetName()
                );

                const auto* sinkPCollection = VerifiedGetSingleSink(transformNode);
                auto* outputTable = PlainGraph_->CreateIntermediateTable(
                    mergeOperation,
                    TMergeOutputConnector{},
                    sinkPCollection->GetRowVtable(),
                    Config_->GetWorkingDir(),
                    Config_->GetEnableProtoFormatForIntermediates()
                );
                RegisterPCollection(sinkPCollection, outputTable);
                break;
            }
            case ERawTransformType::StatefulTimerParDo:
            case ERawTransformType::CombineGlobally:
            case ERawTransformType::StatefulParDo:
                Y_ABORT("Not implemented yet");
        }
    }

    std::shared_ptr<TYtGraphV2> Build()
    {
        return std::make_shared<TYtGraphV2>(std::move(PlainGraph_), *Config_);
    }

private:
    void RegisterPCollection(const TPCollectionNode* pCollection, TTableNode* image)
    {
        auto inserted = PCollectionMap_.emplace(pCollection, std::move(image)).second;
        Y_ABORT_UNLESS(inserted);
    }

    TTableNode* GetPCollectionImage(const TPCollectionNode* pCollection) const
    {
        auto it = PCollectionMap_.find(pCollection);
        Y_ABORT_UNLESS(it != PCollectionMap_.end());
        return it->second;
    }

private:
    const std::shared_ptr<const TYtPipelineConfig> Config_;

    std::unique_ptr<TYtGraphV2::TPlainGraph> PlainGraph_ = std::make_unique<TYtGraphV2::TPlainGraph>();

    THashMap<const TPCollectionNode*, TTableNode*> PCollectionMap_;
};

////////////////////////////////////////////////////////////////////////////////

TYtGraphV2::TYtGraphV2(std::unique_ptr<TPlainGraph> plainGraph, const TYtPipelineConfig& config)
    : PlainGraph_(std::move(plainGraph))
    , Config_(config)
{ }

TYtGraphV2::~TYtGraphV2()
{ }

void TYtGraphV2::Optimize()
{
    TMapFuser mapFuser;
    *PlainGraph_ = mapFuser.Optimize(*PlainGraph_);

    TMapReduceFuser mapReduceFuser;
    *PlainGraph_ = mapReduceFuser.Optimize(*PlainGraph_);
}

std::vector<std::vector<IYtGraph::TOperationNodeId>> TYtGraphV2::GetOperationLevels() const
{
    std::vector<const TTableNode*> readyTables;

    THashMap<const TOperationNode*, std::set<const TTableNode*>> dependencyMap;
    THashMap<const TOperationNode*, std::vector<const TTableNode*>> outputMap;
    THashMap<const TOperationNode*, IYtGraph::TOperationNodeId> idMap;

    for (const auto& tableNode : PlainGraph_->Tables) {
        if (tableNode->GetTableType() == ETableType::Input) {
            readyTables.push_back(tableNode.get());
        }
    }

    for (ssize_t i = 0; i < std::ssize(PlainGraph_->Operations); ++i) {
        const auto& operationNode = PlainGraph_->Operations[i];
        idMap[operationNode.get()] = i;
        dependencyMap[operationNode.get()].insert(
            operationNode->InputTables.begin(),
            operationNode->InputTables.end());

        auto& curOutputs = outputMap[operationNode.get()];
        for (const auto& item : operationNode->OutputTables) {
            curOutputs.push_back(item.second);
        }
    }

    std::vector<std::vector<IYtGraph::TOperationNodeId>> operationNodeLevels;
    while (!readyTables.empty()) {
        std::vector<const TOperationNode*> readyOperations;

        for (const auto* table : readyTables) {
            for (const auto& inputFor : table->InputFor) {
                auto* operation = inputFor.Operation;
                auto it = dependencyMap.find(operation);
                Y_ABORT_UNLESS(it != dependencyMap.end());
                auto& dependencies = it->second;
                dependencies.erase(table);
                if (dependencies.empty()) {
                    readyOperations.push_back(operation);
                    dependencyMap.erase(operation);
                }
            }

            operationNodeLevels.emplace_back();
            operationNodeLevels.back().reserve(readyOperations.size());
        }

        readyTables.clear();
        for (const auto* operation : readyOperations) {
            operationNodeLevels.back().push_back(idMap[operation]);
            for (const auto& item : operation->OutputTables) {
                readyTables.push_back(item.second);
            }
        }
    }

    return operationNodeLevels;
}

THashMap<IYtGraph::TOperationNodeId, std::vector<IYtGraph::TOperationNodeId>>
TYtGraphV2::GetNextOperationMapping() const
{
    THashMap<const TOperationNode*, IYtGraph::TOperationNodeId> idMap;
    THashMap<IYtGraph::TOperationNodeId, std::vector<IYtGraph::TOperationNodeId>> operationMap;
    for (ssize_t i = 0; i < std::ssize(PlainGraph_->Operations); ++i) {
        const auto& operationNode = PlainGraph_->Operations[i];
        idMap[operationNode.get()] = i;
        operationMap[i] = {};
    }

    for (ssize_t i = 0; i < std::ssize(PlainGraph_->Operations); ++i) {
        const auto& operationNode = PlainGraph_->Operations[i];

        for (const auto& [_, outputTable] : operationNode->OutputTables) {
            for (const auto& nextOperationNode : outputTable->InputFor) {
                operationMap[idMap[operationNode.get()]].push_back(idMap[nextOperationNode.Operation]);
            }
        }
    }

    return operationMap;
}

static void PatchOperationSpec(NYT::TNode* result, const NYT::TNode& patch, const TString& currentPath = {})
{
    if (result->IsUndefined()) {
        *result = patch;
    }
    if (!patch.IsMap()) {
        if (*result == patch) {
            return;
        } else {
            ythrow yexception() << "Conflicting values for key " << currentPath << '\n'
                << NYT::NodeToYsonString(*result) << '\n'
                << NYT::NodeToYsonString(patch);
        }
    }

    Y_ABORT_UNLESS(patch.IsMap());
    if (!result->IsMap()) {
            ythrow yexception() << "Conflicting types for key " << currentPath << '\n'
                << result->GetType() << " and " << patch.GetType();
    }

    for (auto&& [key, value] : patch.AsMap()) {
        // Perfect code would escape '/' char in `key` but we are not perfect :(
        auto newPath = currentPath + "/" + key;
        PatchOperationSpec(&(*result)[key], value, newPath);
    }
}

static NYT::TNode GetOperationSpecPatch(const THashSet<TString>& transformNames, const TYtPipelineConfig& config)
{
    auto normalizeName = [] (const TString& input) {
        TString result;
        if (!input.StartsWith('/')) {
            result.push_back('/');
        }
        result += input;
        if (!result.EndsWith('/')) {
            result.push_back('/');
        }
        return result;
    };

    // This is not the fastest algorithm, but we don't expect large amount of config patches here
    // so it should work ok.
    NYT::TNode result;

    std::vector<TString> normalizedTransformNameList;
    for (const auto& transformName : transformNames) {
        normalizedTransformNameList.push_back(normalizeName(transformName));
    }

    for (const auto& [namePattern, operationConfig] : config.GetOperatinonConfig()) {
        auto normalizedNamePattern = normalizeName(namePattern);
        for (const auto& normalizedTransformName : normalizedTransformNameList) {
            if (normalizedTransformName.find(normalizedNamePattern) != TString::npos) {
                auto patch = NYT::NodeFromYsonString(operationConfig.GetSpecPatch());
                PatchOperationSpec(&result, patch);
                break;
            }
        }
    }
    return result;
}

NYT::IOperationPtr TYtGraphV2::StartOperation(const NYT::IClientBasePtr& client, TOperationNodeId nodeId, const TStartOperationContext& context) const
{
    auto addLocalFiles = [] (const IRawParDoPtr& parDo, NYT::TUserJobSpec* spec) {
        const auto& resourceFileList = TFnAttributesOps::GetResourceFileList(parDo->GetFnAttributes());

        for (const auto& resourceFile : resourceFileList) {
            spec->AddLocalFile(resourceFile);
        }
    };

    Y_ABORT_UNLESS(nodeId >= 0 && nodeId < std::ssize(PlainGraph_->Operations));
    const auto& operation = PlainGraph_->Operations[nodeId];

    auto operationOptions = NYT::TOperationOptions().Wait(false);
    auto specPatch = GetOperationSpecPatch(operation->GetTransformNames(), *context.Config);
    if (!specPatch.IsUndefined()) {
        operationOptions.Spec(specPatch);
    }

    switch (operation->GetOperationType()) {
        case EOperationType::Merge:
        case EOperationType::Map: {
            const auto* mapOperation = operation->GetOperationType() == EOperationType::Map
                ? operation->VerifiedAsPtr<TMapOperationNode>()
                : nullptr;

            if (!mapOperation || mapOperation->GetMapperBuilder().Empty()) {
                TTableNode* outputTable;
                Y_ABORT_UNLESS(operation->OutputTables.size() == 1);
                for (const auto& [connector, table] : operation->OutputTables) {
                    outputTable = table;
                }

                THashSet<ETableType> inputTableTypes;
                for (const auto* table : operation->InputTables) {
                    inputTableTypes.insert(table->GetTableType());
                }

                if (
                    inputTableTypes == THashSet<ETableType>{ETableType::Intermediate}
                    && outputTable->GetTableType() == ETableType::Intermediate
                ) {
                    NYT::TMergeOperationSpec spec;
                    for (const auto *table : operation->InputTables) {
                        spec.AddInput(table->GetPath());
                    }
                    Y_ABORT_UNLESS(std::ssize(operation->OutputTables) == 1);
                    for (const auto &[_, table] : operation->OutputTables) {
                        spec.Output(table->GetPath());
                    }
                    return client->Merge(spec, operationOptions);
                } else {
                    NYT::TRawMapOperationSpec spec;
                    TParDoTreeBuilder mapperBuilder;
                    auto impulseOutputIdList = mapperBuilder.AddParDo(
                        CreateReadImpulseParDo(operation->InputTables),
                        TParDoTreeBuilder::RootNodeId
                    );
                    Y_ABORT_UNLESS(impulseOutputIdList.size() == operation->InputTables.size());

                    for (ssize_t i = 0; i < std::ssize(impulseOutputIdList); ++i) {
                        auto* inputTable = operation->InputTables[i];
                        mapperBuilder.AddParDoChainVerifyNoOutput(
                            impulseOutputIdList[i],
                            {
                                inputTable->CreateDecodingParDo(),
                                outputTable->CreateEncodingParDo(),
                                outputTable->CreateWriteParDo(i),
                            }
                        );
                        spec.AddInput(inputTable->GetPath());
                    }

                    switch (outputTable->GetTableType()) {
                        case ETableType::Input:
                            Y_ABORT("Output table cannot have type ETableType::Input");
                        case ETableType::Intermediate:
                            spec.AddOutput(outputTable->GetPath());
                            break;
                        case ETableType::Output: {
                            NYT::TRichYPath outputPath = outputTable->GetPath();
                            spec.AddOutput(
                                NYT::TRichYPath(outputTable->GetPath())
                                    .Schema(outputTable->VerifiedAsPtr<TOutputTableNode>()->GetSchema())
                            );
                            break;
                        }
                    }

                    spec.InputFormat(GetInputFormat(operation->InputTables));
                    spec.OutputFormat(GetOutputFormat(operation->OutputTables));

                    NYT::IRawJobPtr job = CreateImpulseJob(mapperBuilder.Build());
                    return client->RawMap(spec, job, operationOptions);
                }
            } else {
                NYT::TRawMapOperationSpec spec;
                Y_ABORT_UNLESS(operation->InputTables.size() == 1);
                const auto* inputTable = operation->InputTables[0];
                spec.AddInput(inputTable->GetPath());

                TParDoTreeBuilder tmpMapBuilder = mapOperation->GetMapperBuilder();
                std::vector<IYtJobOutputPtr> jobOutputs;
                ssize_t sinkIndex = 0;
                for (const auto &[connector, table] : operation->OutputTables) {
                    Y_ABORT_UNLESS(std::holds_alternative<TMapperOutputConnector>(connector));
                    const auto& mapperConnector = std::get<TMapperOutputConnector>(connector);
                    Y_ABORT_UNLESS(mapperConnector.MapperIndex == 0);
                    auto output = table->GetPath();
                    if (auto* outputTable = table->TryAsPtr<TOutputTableNode>()) {
                        output.Schema(outputTable->GetSchema());
                    }
                    spec.AddOutput(table->GetPath());
                    auto nodeId = tmpMapBuilder.AddParDoVerifySingleOutput(
                        table->CreateEncodingParDo(),
                        mapperConnector.NodeId
                    );
                    tmpMapBuilder.AddParDoVerifyNoOutput(
                        table->CreateWriteParDo(sinkIndex),
                        nodeId
                    );

                    ++sinkIndex;
                }
                TParDoTreeBuilder mapperBuilder;
                auto nodeId = mapperBuilder.AddParDoVerifySingleOutput(
                    CreateReadImpulseParDo(operation->InputTables),
                    TParDoTreeBuilder::RootNodeId
                );
                nodeId = mapperBuilder.AddParDoVerifySingleOutput(
                    inputTable->CreateDecodingParDo(),
                    nodeId
                );
                mapperBuilder.Fuse(tmpMapBuilder, nodeId);

                spec.InputFormat(GetInputFormat(operation->InputTables));
                spec.OutputFormat(GetOutputFormat(operation->OutputTables));

                auto mapperParDo = mapperBuilder.Build();
                addLocalFiles(mapperParDo, &spec.MapperSpec_);
                NYT::IRawJobPtr job = CreateImpulseJob(mapperParDo);

                return client->RawMap(spec, job, operationOptions);
            }
        }
        case EOperationType::MapReduce: {
            const auto* mapReduceOperation = operation->VerifiedAsPtr<TMapReduceOperationNode>();

            Y_ABORT_UNLESS(std::ssize(operation->InputTables) == std::ssize(mapReduceOperation->GetMapperBuilderList()));

            NYT::TRawMapReduceOperationSpec spec;
            auto tmpMapperBuilderList = mapReduceOperation->GetMapperBuilderList();
            for (const auto* inputTable : operation->InputTables) {
                spec.AddInput(inputTable->GetPath());
            }

            auto reducerBuilder = mapReduceOperation->GetReducerBuilder();
            ssize_t mapperOutputIndex = 1;
            ssize_t reducerOutputIndex = 0;
            for (const auto& item : mapReduceOperation->OutputTables) {
                const auto& connector = item.first;
                const auto& outputTable = item.second;
                std::visit([&] (const auto& connector) {
                    using TType = std::decay_t<decltype(connector)>;
                    if constexpr (std::is_same_v<TType, TMergeOutputConnector>) {
                        Y_ABORT("Unexpected TMergeOutputConnector inside MapReduce operation");
                    } else if constexpr (std::is_same_v<TType, TMapperOutputConnector>) {
                        tmpMapperBuilderList[connector.MapperIndex].AddParDoChainVerifyNoOutput(
                            connector.NodeId,
                            {
                                outputTable->CreateEncodingParDo(),
                                outputTable->CreateWriteParDo(mapperOutputIndex)
                            }
                        );
                        spec.AddMapOutput(outputTable->GetPath());
                        ++mapperOutputIndex;
                    } else if constexpr (std::is_same_v<TType, TReducerOutputConnector>) {
                        reducerBuilder.AddParDoChainVerifyNoOutput(
                            connector.NodeId, {
                                outputTable->CreateEncodingParDo(),
                                outputTable->CreateWriteParDo(reducerOutputIndex)
                            }
                        );
                        spec.AddOutput(outputTable->GetPath());
                        ++reducerOutputIndex;
                    } else {
                        static_assert(std::is_same_v<TType, TMapperOutputConnector>);
                    }
                }, connector);
            }

            TParDoTreeBuilder mapBuilder;
            auto impulseOutputIdList = mapBuilder.AddParDo(
                CreateReadImpulseParDo(operation->InputTables),
                TParDoTreeBuilder::RootNodeId
            );
            Y_ABORT_UNLESS(impulseOutputIdList.size() == tmpMapperBuilderList.size());
            Y_ABORT_UNLESS(impulseOutputIdList.size() == operation->InputTables.size());
            for (ssize_t i = 0; i < std::ssize(impulseOutputIdList); ++i) {
                auto decodedId = mapBuilder.AddParDoVerifySingleOutput(
                    operation->InputTables[i]->CreateDecodingParDo(),
                    impulseOutputIdList[i]
                );
                mapBuilder.Fuse(tmpMapperBuilderList[i], decodedId);
            }

            auto mapperParDo = mapBuilder.Build();
            addLocalFiles(mapperParDo, &spec.MapperSpec_);
            auto mapperJob = CreateImpulseJob(mapperParDo);

            auto reducerParDo = reducerBuilder.Build();
            addLocalFiles(reducerParDo, &spec.ReducerSpec_);
            auto reducerJob = CreateImpulseJob(reducerParDo);

            NYT::IRawJobPtr combinerJob = nullptr;
            if (!mapReduceOperation->GetCombinerBuilder().Empty()) {
                spec.ForceReduceCombiners(true);
                auto combinerBuilder = mapReduceOperation->GetCombinerBuilder();
                auto combinerParDo = combinerBuilder.Build();
                addLocalFiles(combinerParDo, &spec.ReduceCombinerSpec_);
                combinerJob = CreateImpulseJob(combinerParDo);
            }

            spec.MapperInputFormat(GetInputFormat(mapReduceOperation->InputTables));
            spec.MapperOutputFormat(GetIntermediatesFormat(Config_.GetEnableProtoFormatForIntermediates()));
            spec.ReducerInputFormat(GetIntermediatesFormat(Config_.GetEnableProtoFormatForIntermediates()));
            spec.ReducerOutputFormat(GetOutputFormat(mapReduceOperation->OutputTables));
            spec.ReduceBy({"key"});

            return client->RawMapReduce(
                spec,
                mapperJob,
                nullptr,
                reducerJob,
                operationOptions
            );
        }
    }
    Y_ABORT("Unknown operation");
}

TString TYtGraphV2::DumpDOTSubGraph(const TString&) const
{
    Y_ABORT("Not implemented yet");
    return TString{};
}

TString TYtGraphV2::DumpDOT(const TString&) const
{
    Y_ABORT("Not implemented yet");
    return TString{};
}

std::set<TString> TYtGraphV2::GetEdgeDebugStringSet() const
{
    std::set<TString> result;
    for (const auto& table : PlainGraph_->Tables) {
        if (table->GetTableType() == ETableType::Input) {
            for (const auto& inputFor : table->InputFor) {
                auto edge = NYT::Format("%v -> %v", table->GetPath().Path_, inputFor.Operation->GetFirstName());
                result.insert(std::move(edge));
            }
        }
    }

    for (const auto& operation : PlainGraph_->Operations) {
        for (const auto& [connector, outputTable] : operation->OutputTables) {
            switch (outputTable->GetTableType()) {
                case ETableType::Output: {
                    TStringStream tableRepresentation;
                    {
                        bool first = true;
                        auto pathSchemaList = outputTable->VerifiedAsPtr<TOutputTableNode>()->GetPathSchemaList();
                        for (const auto& pair : pathSchemaList) {
                            if (first) {
                                first = false;
                            } else {
                                tableRepresentation << ", ";
                            }
                            tableRepresentation << pair.first.Path_;
                        }
                    }
                    auto edge = NYT::Format("%v -> %v", operation->GetFirstName(), tableRepresentation.Str());
                    result.insert(std::move(edge));
                    break;
                }
                case ETableType::Intermediate: {
                    for (const auto& inputFor : outputTable->InputFor) {
                        auto edge = NYT::Format("%v -> %v", operation->GetFirstName(), inputFor.Operation->GetFirstName());
                        result.insert(std::move(edge));
                    }
                    break;
                }
                case ETableType::Input:
                    Y_ABORT("Unexpected");
            }
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TYtGraphV2> BuildYtGraphV2(const TPipeline& pipeline, const TYtPipelineConfig& config)
{
    TYtGraphV2Builder builder{config};
    TraverseInTopologicalOrder(GetRawPipeline(pipeline), &builder);
    return builder.Build();
}

std::shared_ptr<TYtGraphV2> BuildOptimizedYtGraphV2(const TPipeline& pipeline, const TYtPipelineConfig& config)
{
    auto graph = BuildYtGraphV2(pipeline, config);
    graph->Optimize();
    return graph;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
