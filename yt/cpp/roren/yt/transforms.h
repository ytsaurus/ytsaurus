#pragma once

#include "yt_io_private.h"
#include "yt_proto_io.h"
#include "tables.h"

#include <yt/cpp/mapreduce/interface/common.h>

#include <yt/cpp/roren/interface/transforms.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

class TYtWriteTransform
    : public NPrivate::IWithAttributes
{
public:
    TYtWriteTransform(
        const NYT::TRichYPath& path,
        const NYT::TTableSchema& schema,
        const NYT::TSortColumns& columnsToSort,
        bool uniqueKeys)
        : Path_(path)
        , Schema_(schema)
        , ColumnsToSort_(columnsToSort)
        , UniqueKeys_(uniqueKeys)
        , RawWrite_(MakeIntrusive<NPrivate::TRawDummyWriter>(NPrivate::MakeRowVtable<void>()))
    { }

    TYtWriteTransform(
        const NYT::TRichYPath& path,
        const NYT::TTableSchema& schema)
        : Path_(path)
        , Schema_(schema)
        , ColumnsToSort_(std::nullopt)
        , RawWrite_(MakeIntrusive<NPrivate::TRawDummyWriter>(NPrivate::MakeRowVtable<void>()))
    { }

    TString GetName() const
    {
        return "Write";
    }

    template <typename TInputRow>
    void ApplyTo(const TPCollection<TInputRow>& pCollection) const
    {
        RawWrite_ = ColumnsToSort_ ? CreateSortedWrite<TInputRow>() : CreateWrite<TInputRow>();

        const auto& rawPipeline = NPrivate::GetRawPipeline(pCollection);
        auto* rawInputNode = NPrivate::GetRawDataNode(pCollection).Get();
        auto transformNode = rawPipeline->AddTransform(RawWrite_, {rawInputNode});
        Y_ABORT_UNLESS(transformNode->GetTaggedSinkNodeList().size() == 0);
    }

private:
    void SetAttribute(const TString& key, const std::any& value) override
    {
        RawWrite_->SetAttribute(key, value);
    }

    const std::any* GetAttribute(const TString& key) const override
    {
        return RawWrite_->GetAttribute(key);
    }

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

    template <class TInputRow>
    NPrivate::IRawYtWritePtr  CreateSortedWrite() const
    {
        if constexpr (std::is_same_v<TInputRow, NYT::TNode>) {
            return NPrivate::MakeYtNodeSortedWrite(Path_, Schema_, *ColumnsToSort_, UniqueKeys_);
        } else if constexpr (std::is_base_of_v<::google::protobuf::Message, TInputRow>) {
            return NPrivate::MakeYtProtoSortedWrite<TInputRow>(Path_, Schema_, *ColumnsToSort_, UniqueKeys_);
        } else {
            static_assert(TDependentFalse<TInputRow>, "unknown YT writer");
        }
    }

private:
    NYT::TRichYPath Path_;
    NYT::TTableSchema Schema_;
    std::optional<NYT::TSortColumns> ColumnsToSort_;
    bool UniqueKeys_;

    mutable NPrivate::IRawWritePtr RawWrite_;
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
