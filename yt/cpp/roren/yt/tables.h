#pragma once

#include "connectors.h"
#include "node.h"
#include "operations.h"
#include "yt_io_private.h"
#include "yt_graph_v2.h"
#include "yt_proto_io.h"

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

using TTableNode = TYtGraphV2::TTableNode;
using TTableNodePtr = TYtGraphV2::TTableNodePtr;

////////////////////////////////////////////////////////////////////////////////

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

class TInputTableNode
    : public virtual TYtGraphV2::TTableNode
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
    : public virtual TYtGraphV2::TTableNode
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

protected:
    const IRawYtWritePtr RawYtWrite_;
    std::vector<IRawYtWritePtr> SecondaryYtWriteList_;
};

////////////////////////////////////////////////////////////////////////////////

class TIntermediateTableNode
    : public virtual TYtGraphV2::TTableNode
{
public:
    TIntermediateTableNode(TRowVtable vtable, TString temporaryDirectory, bool useProtoFormat)
        : TYtGraphV2::TTableNode(std::move(vtable))
        , TemporaryDirectory_(std::move(temporaryDirectory))
        , Guid_(CreateGuidAsString())
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
        tableName << "." << ConnectorToString(OutputOf.Connector);

        return TemporaryDirectory_ + "/" + tableName.Str() + ".GUID-" + Guid_;
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

protected:
    const TString TemporaryDirectory_;

private:
    const TString Guid_;
    bool UseProtoFormat_;
};

////////////////////////////////////////////////////////////////////////////////

class TTypedIntermediateTableNode
    : public TOutputTableNode
    , public TIntermediateTableNode
{
public:
    TTypedIntermediateTableNode(TRowVtable vtable, TString temporaryDirectory, IRawYtWritePtr rawYtWrite)
        : TTableNode(vtable)
        , TOutputTableNode(vtable, rawYtWrite)
        , TIntermediateTableNode(
            vtable,
            temporaryDirectory,
            TOutputTableNode::GetTableFormat() == ETableFormat::Proto)
    { }

    ETableType GetTableType() const override
    {
        return TIntermediateTableNode::GetTableType();
    }

    ETableFormat GetTableFormat() const override
    {
        return TOutputTableNode::GetTableFormat();
    }

    IRawParDoPtr CreateDecodingParDo() const override
    {
        return TOutputTableNode::CreateDecodingParDo();
    }

    IRawParDoPtr CreateWriteParDo(ssize_t tableIndex) const override
    {
        return TOutputTableNode::CreateWriteParDo(tableIndex);
    }

    IRawParDoPtr CreateEncodingParDo() const override
    {
        return TOutputTableNode::CreateEncodingParDo();
    }

    NYT::TRichYPath GetPath() const override
    {
        auto path = TIntermediateTableNode::GetPath();
        path.Schema(RawYtWrite_->GetSchema());
        if (!path.OptimizeFor_.Defined()) {
            path.OptimizeFor_ = NYT::EOptimizeForAttr::OF_SCAN_ATTR;
        }

        return path;
    }

    const ::google::protobuf::Descriptor* GetProtoDescriptor() const override
    {
        return TOutputTableNode::GetProtoDescriptor();
    }

private:
    std::shared_ptr<TTableNode> Clone() const override
    {
        return std::make_shared<TTypedIntermediateTableNode>(TIntermediateTableNode::Vtable, TemporaryDirectory_, RawYtWrite_);
    }
};

////////////////////////////////////////////////////////////////////////////////

NYT::TFormat GetFormat(const std::vector<TTableNode*>& tables);
NYT::TFormat GetFormat(const THashMap<TOperationConnector, TTableNode*>& tables);
NYT::TFormat GetFormatWithIntermediate(bool useProtoFormat, const std::vector<TTableNode*>& tables);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
