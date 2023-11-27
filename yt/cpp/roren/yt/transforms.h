#pragma once

#include "yt_io_private.h"
#include "yt_proto_io.h"

#include <yt/cpp/mapreduce/interface/common.h>

#include <yt/cpp/roren/interface/transforms.h>

namespace NRoren {

class TYtWriteTransform
    : public NPrivate::IWithAttributes
{
public:
    TYtWriteTransform(
        const NYT::TRichYPath& path,
        const NYT::TTableSchema& schema,
        const std::vector<std::string>& columnsToSort)
        : Path_(path)
        , Schema_(schema)
        , ColumnsToSort_(columnsToSort)
    { }

    TYtWriteTransform(
        const NYT::TRichYPath& path,
        const NYT::TTableSchema& schema)
        : Path_(path)
        , Schema_(schema)
        , ColumnsToSort_(std::nullopt)
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
            return nullptr;
        } else if constexpr (std::is_base_of_v<::google::protobuf::Message, TInputRow>) {
            return NPrivate::MakeYtProtoSortedWrite<TInputRow>(Path_, Schema_, *ColumnsToSort_);
        } else {
            static_assert(TDependentFalse<TInputRow>, "unknown YT writer");
        }
    }

private:
    NYT::TRichYPath Path_;
    NYT::TTableSchema Schema_;
    std::optional<std::vector<std::string>> ColumnsToSort_;

    mutable NPrivate::IRawYtWritePtr RawWrite_;
};

} // namespace NRoren::NPrivate
