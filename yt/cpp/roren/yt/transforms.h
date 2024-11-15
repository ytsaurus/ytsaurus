#pragma once

#include "yt_io_private.h"
#include "yt_proto_io.h"
#include "tables.h"

#include <yt/cpp/mapreduce/interface/common.h>

#include <yt/cpp/roren/interface/transforms.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TYtWriteTransform
    : public NPrivate::TAttributes
{
public:
    TYtWriteTransform(
        const NYT::TRichYPath& path,
        const NYT::TTableSchema& schema)
        : Path_(path)
        , Schema_(schema)
    { }

    TString GetName() const
    {
        return "Write";
    }

    template <typename TInputRow>
    void ApplyTo(const TPCollection<TInputRow>& pCollection) const
    {
        const auto& rawPipeline = NPrivate::GetRawPipeline(pCollection);
        auto* rawInputNode = NPrivate::GetRawDataNode(pCollection).Get();
        auto rawWrite = CreateWrite<TInputRow>();
        NPrivate::MergeAttributes(*rawWrite, *this);
        auto transformNode = rawPipeline->AddTransform(rawWrite, {rawInputNode});
        Y_ABORT_UNLESS(transformNode->GetTaggedSinkNodeList().size() == 0);
    }

private:
    template <class TInputRow>
    NPrivate::IRawYtWritePtr  CreateWrite() const
    {
        if constexpr (std::is_same_v<TInputRow, NYT::TNode>) {
            return NPrivate::MakeYtNodeWrite(Path_, Schema_);
        } else if constexpr (std::is_base_of_v<::google::protobuf::Message, TInputRow>) {
            return NPrivate::MakeYtProtoWrite<TInputRow>(Path_, Schema_);
        } else {
            static_assert(TDependentFalse<TInputRow>, "unknown YT write");
        }
    }

private:
    const NYT::TRichYPath Path_;
    const NYT::TTableSchema Schema_;
};

class TYtSortedWriteTransform
    : public NPrivate::TAttributes
{
public:
    TYtSortedWriteTransform(NYT::TRichYPath path, NYT::TTableSchema schema)
        : Path_(std::move(path))
        , Schema_(std::move(schema))
    { }

    TString GetName() const
    {
        return "Write";
    }

    template <typename TInputRow>
    void ApplyTo(const TPCollection<TInputRow>& pCollection) const
    {
        const auto& rawPipeline = NPrivate::GetRawPipeline(pCollection);
        auto* rawInputNode = NPrivate::GetRawDataNode(pCollection).Get();
        auto rawWrite = CreateSortedWrite<TInputRow>();
        NPrivate::MergeAttributes(*rawWrite, *this);
        auto transformNode = rawPipeline->AddTransform(rawWrite, {rawInputNode});
        Y_ABORT_UNLESS(transformNode->GetTaggedSinkNodeList().size() == 0);
    }

private:
    template <class TInputRow>
    NPrivate::IRawYtWritePtr  CreateSortedWrite() const
    {
        if constexpr (std::is_same_v<TInputRow, NYT::TNode>) {
            return NPrivate::MakeYtNodeSortedWrite(Path_, Schema_);
        } else if constexpr (std::is_base_of_v<::google::protobuf::Message, TInputRow>) {
            return NPrivate::MakeYtProtoSortedWrite<TInputRow>(Path_, Schema_);
        } else {
            static_assert(TDependentFalse<TInputRow>, "unknown YT writer");
        }
    }

private:
    const NYT::TRichYPath Path_;
    const NYT::TTableSchema Schema_;
};

class TYtAutoSchemaWriteTransform
    : public NPrivate::TAttributes
{
public:
    TYtAutoSchemaWriteTransform(NYT::TRichYPath path)
        : Path_(path)
    { }

    TString GetName() const
    {
        return "AutoSchemaWrite";
    }

    template <typename TInputRow>
        requires std::is_base_of_v<::google::protobuf::Message, TInputRow>
    void ApplyTo(const TPCollection<TInputRow>& pCollection) const
    {
        auto schema = NYT::CreateTableSchema<TInputRow>();
        auto transform = TYtWriteTransform(Path_, schema);
        NPrivate::MergeAttributes(transform, *this);
        pCollection | transform;
    }

private:
    const NYT::TRichYPath Path_;
};

class TYtAutoSchemaSortedWriteTransform
    : public NPrivate::TAttributes
{
public:
    TYtAutoSchemaSortedWriteTransform(NYT::TRichYPath path, NYT::TSortColumns sortColumns)
        : Path_(std::move(path))
        , SortColumns_(std::move(sortColumns))
    { }

    TString GetName() const
    {
        return "AutoSchemaWrite";
    }

    template <typename TInputRow>
        requires std::is_base_of_v<::google::protobuf::Message, TInputRow>
    void ApplyTo(const TPCollection<TInputRow>& pCollection) const
    {
        auto schema = NYT::CreateTableSchema<TInputRow>(SortColumns_);
        auto transform = TYtSortedWriteTransform(Path_, schema);
        NPrivate::MergeAttributes(transform, *this);
        pCollection | transform;
    }

private:
    const NYT::TRichYPath Path_;
    const NYT::TSortColumns SortColumns_;
};

////////////////////////////////////////////////////////////////////////////////

NPrivate::IRawParDoPtr CreateAddTableIndexProtoParDo(ssize_t index);
NPrivate::IRawParDoPtr CreateAddTableIndexParDo(ssize_t index);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateReadImpulseParDo(const std::vector<TTableNode*>& inputTables);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
