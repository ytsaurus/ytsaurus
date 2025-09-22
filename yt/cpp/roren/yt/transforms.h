#pragma once

#include "yt_io_private.h"
#include "yt_proto_io.h"
#include "tables.h"

#include <yt/cpp/mapreduce/interface/common.h>

#include <yt/cpp/roren/interface/transforms.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TYtWriteApplicator
    : public NPrivate::TAttributes
{
public:
    TYtWriteApplicator(const NYT::TRichYPath& path, const NYT::TTableSchema& schema);

    TString GetName() const;

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

class TYtSortedWriteApplicator
    : public NPrivate::TAttributes
{
public:
    TYtSortedWriteApplicator(NYT::TRichYPath path, NYT::TTableSchema schema);

    TString GetName() const;

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

class TYtAutoSchemaWriteApplicator
    : public NPrivate::TAttributes
{
public:
    explicit TYtAutoSchemaWriteApplicator(NYT::TRichYPath path);

    TString GetName() const;

    template <typename TInputRow>
        requires std::is_base_of_v<::google::protobuf::Message, TInputRow>
    void ApplyTo(const TPCollection<TInputRow>& pCollection) const
    {
        auto schema = NYT::CreateTableSchema<TInputRow>();
        auto applicator = TYtWriteApplicator(Path_, schema);
        NPrivate::MergeAttributes(applicator, *this);
        pCollection | applicator;
    }

private:
    const NYT::TRichYPath Path_;
};

class TYtAutoSchemaSortedWriteApplicator
    : public NPrivate::TAttributes
{
public:
    TYtAutoSchemaSortedWriteApplicator(NYT::TRichYPath path, NYT::TSortColumns sortColumns);

    TString GetName() const;

    template <typename TInputRow>
        requires std::is_base_of_v<::google::protobuf::Message, TInputRow>
    void ApplyTo(const TPCollection<TInputRow>& pCollection) const
    {
        auto schema = NYT::CreateTableSchema<TInputRow>(SortColumns_);
        auto applicator = TYtSortedWriteApplicator(Path_, schema);
        NPrivate::MergeAttributes(applicator, *this);
        pCollection | applicator;
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
