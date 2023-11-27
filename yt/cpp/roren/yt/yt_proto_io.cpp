#include "yt_proto_io.h"
#include "jobs.h"

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/io/proto_table_reader.h>
#include <yt/cpp/mapreduce/io/stream_table_reader.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TDecodingValueProtoParDo
    : public IRawParDo
{
public:
    TDecodingValueProtoParDo() = default;

    explicit TDecodingValueProtoParDo(TRowVtable rowVtable)
        : OutputRowVtable_(rowVtable)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TDecodingValueProtoParDo.Input", MakeRowVtable<TKVProto>())};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag("TDecodingValueProtoParDo.Output", OutputRowVtable_)};
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");
        Y_ABORT_UNLESS(outputs.size() == 1);
        Output_ = outputs[0];
        if (!Coder_) {
            Coder_ = OutputRowVtable_.RawCoderFactory();
            OutputRowHolder_.Reset(OutputRowVtable_);
        }
    }

    void Do(const void* rows, int count) override
    {
        const auto* curRow = static_cast<const TKVProto*>(rows);
        for (int i = 0; i < count; ++i, ++curRow) {
            Coder_->DecodeRow(curRow->GetValue(), OutputRowHolder_.GetData());
            Output_->AddRaw(OutputRowHolder_.GetData(), 1);
        }
    }

    void Finish() override
    { }

    const TFnAttributes& GetFnAttributes() const override
    {
        static const TFnAttributes fnAttributes;
        return fnAttributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TDecodingValueProtoParDo>();
        };
    }

private:
    TRowVtable OutputRowVtable_;

    IRawOutputPtr Output_;
    IRawCoderPtr Coder_;
    TRawRowHolder OutputRowHolder_;

    Y_SAVELOAD_DEFINE_OVERRIDE(OutputRowVtable_);
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateDecodingValueProtoParDo(TRowVtable rowVtable)
{
    return ::MakeIntrusive<TDecodingValueProtoParDo>(rowVtable);
}

////////////////////////////////////////////////////////////////////////////////

class TEncodingValueProtoParDo
    : public IRawParDo
{
public:
    TEncodingValueProtoParDo() = default;

    explicit TEncodingValueProtoParDo(TRowVtable rowVtable)
        : InputRowVtable_(rowVtable)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TEncodingValueProtoParDo.Input", InputRowVtable_)};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag("TEncodingValueProtoParDo.Output", MakeRowVtable<TKVProto>())};
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");
        Y_ABORT_UNLESS(outputs.size() == 1);
        Output_ = outputs[0];
        if (!Coder_) {
            Coder_ = InputRowVtable_.RawCoderFactory();
        }
    }

    void Do(const void* rows, int count) override
    {
        const auto* curRow = static_cast<const std::byte*>(rows);
        for (int i = 0; i < count; ++i, curRow += InputRowVtable_.DataSize) {
            Buffer_.clear();
            auto out = TStringOutput(Buffer_);
            Coder_->EncodeRow(&out, curRow);
            ResultProto_.SetValue(Buffer_);
            Output_->AddRaw(&ResultProto_, 1);
        }
    }

    void Finish() override
    { }

    const TFnAttributes& GetFnAttributes() const override
    {
        static const TFnAttributes fnAttributes;
        return fnAttributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TEncodingValueProtoParDo>();
        };
    }

private:
    TRowVtable InputRowVtable_;

    IRawOutputPtr Output_;
    IRawCoderPtr Coder_;
    TKVProto ResultProto_;
    TString Buffer_;

    Y_SAVELOAD_DEFINE_OVERRIDE(InputRowVtable_);
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateEncodingValueProtoParDo(TRowVtable rowVtable)
{
    return ::MakeIntrusive<TEncodingValueProtoParDo>(rowVtable);
}

////////////////////////////////////////////////////////////////////////////////

class TDecodingKeyValueProtoParDo
    : public IRawParDo
{
public:
    TDecodingKeyValueProtoParDo() = default;

    explicit TDecodingKeyValueProtoParDo(TRowVtable rowVtable)
        : OutputRowVtable_(rowVtable)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TDecodingKeyValueProtoParDo.Input", MakeRowVtable<TKVProto>())};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag("TDecodingKeyValueProtoParDo.Output", OutputRowVtable_)};
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");
        Y_ABORT_UNLESS(outputs.size() == 1);
        Output_ = outputs[0];
        if (!KeyCoder_) {
            KeyCoder_ = OutputRowVtable_.KeyVtableFactory().RawCoderFactory();
            ValueCoder_ = OutputRowVtable_.ValueVtableFactory().RawCoderFactory();
            OutputRowHolder_.Reset(OutputRowVtable_);
        }
    }

    void Do(const void* rows, int count) override
    {
        const auto* curRow = static_cast<const TKVProto*>(rows);
        for (int i = 0; i < count; ++i, ++curRow) {
            KeyCoder_->DecodeRow(curRow->GetKey(), OutputRowHolder_.GetKeyOfKV());
            ValueCoder_->DecodeRow(curRow->GetValue(), OutputRowHolder_.GetValueOfKV());
            Output_->AddRaw(OutputRowHolder_.GetData(), 1);
        }
    }

    void Finish() override
    { }

    const TFnAttributes& GetFnAttributes() const override
    {
        static const TFnAttributes fnAttributes;
        return fnAttributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TDecodingKeyValueProtoParDo>();
        };
    }

private:
    TRowVtable OutputRowVtable_;

    IRawOutputPtr Output_;
    IRawCoderPtr KeyCoder_;
    IRawCoderPtr ValueCoder_;
    TRawRowHolder OutputRowHolder_;

    Y_SAVELOAD_DEFINE_OVERRIDE(OutputRowVtable_);
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateDecodingKeyValueProtoParDo(TRowVtable rowVtable)
{
    return ::MakeIntrusive<TDecodingKeyValueProtoParDo>(rowVtable);
}

////////////////////////////////////////////////////////////////////////////////

class TEncodingKeyValueProtoParDo
    : public IRawParDo
{
public:
    TEncodingKeyValueProtoParDo() = default;

    explicit TEncodingKeyValueProtoParDo(TRowVtable rowVtable)
        : InputRowVtable_(rowVtable)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TEncodingKeyValueProtoParDo.Input", InputRowVtable_)};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag("TEncodingKeyValueProtoParDo.Output", MakeRowVtable<TKVProto>())};
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");
        Y_ABORT_UNLESS(outputs.size() == 1);
        Output_ = outputs[0];
        if (!KeyCoder_) {
            KeyCoder_ = InputRowVtable_.KeyVtableFactory().RawCoderFactory();
            ValueCoder_ = InputRowVtable_.ValueVtableFactory().RawCoderFactory();
        }
    }

    void Do(const void* rows, int count) override
    {
        const auto* curRow = static_cast<const std::byte*>(rows);
        for (int i = 0; i < count; ++i, curRow += InputRowVtable_.DataSize) {
            KeyBuffer_.clear();
            ValueBuffer_.clear();
            {
                auto keyOut = TStringOutput(KeyBuffer_);
                auto valueOut = TStringOutput(ValueBuffer_);
                KeyCoder_->EncodeRow(&keyOut, GetKeyOfKv(InputRowVtable_, curRow));
                ValueCoder_->EncodeRow(&valueOut, GetValueOfKv(InputRowVtable_, curRow));
            }
            ResultProto_.SetKey(KeyBuffer_);
            ResultProto_.SetValue(ValueBuffer_);
            Output_->AddRaw(&ResultProto_, 1);
        }
    }

    void Finish() override
    { }

    const TFnAttributes& GetFnAttributes() const override
    {
        static const TFnAttributes fnAttributes;
        return fnAttributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TEncodingKeyValueProtoParDo>();
        };
    }

private:
    TRowVtable InputRowVtable_;

    IRawOutputPtr Output_;
    IRawCoderPtr KeyCoder_;
    IRawCoderPtr ValueCoder_;
    TKVProto ResultProto_;
    TString KeyBuffer_;
    TString ValueBuffer_;

    Y_SAVELOAD_DEFINE_OVERRIDE(InputRowVtable_);
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateEncodingKeyValueProtoParDo(TRowVtable rowVtable)
{
    return ::MakeIntrusive<TEncodingKeyValueProtoParDo>(rowVtable);
}

////////////////////////////////////////////////////////////////////////////////

class TSplitKvJobProtoInput
    : public IYtNotSerializableJobInput
{
public:
    TSplitKvJobProtoInput() = default;

    explicit TSplitKvJobProtoInput(const std::vector<TRowVtable>& rowVtables, NYT::TTableReaderPtr<TKVProto> tableReader)
        : RowVtables_(rowVtables)
        , TableReader_(std::move(tableReader))
    {
        KeyDecoders_.reserve(RowVtables_.size());
        ValueDecoders_.reserve(RowVtables_.size());
        RowHolders_.reserve(RowVtables_.size());

        for (const auto& rowVtable : RowVtables_) {
            KeyDecoders_.emplace_back(rowVtable.KeyVtableFactory().RawCoderFactory());
            ValueDecoders_.emplace_back(rowVtable.ValueVtableFactory().RawCoderFactory());
            RowHolders_.emplace_back(rowVtable);
        }
    }

    const void* NextRaw() override
    {
        if (TableReader_->IsValid()) {
            auto msg = TableReader_->GetRow();
            if (msg.HasTableIndex()) {
                TableIndex_ = msg.GetTableIndex();
            }
            Y_ENSURE(TableIndex_ < ssize(RowVtables_));

            auto& rowHolder = RowHolders_[TableIndex_];

            KeyDecoders_[TableIndex_]->DecodeRow(msg.GetKey(), rowHolder.GetKeyOfKV());
            ValueDecoders_[TableIndex_]->DecodeRow(msg.GetValue(), rowHolder.GetValueOfKV());

            TableReader_->Next();
            return rowHolder.GetData();
        } else {
            return nullptr;
        }
    }

    ssize_t GetInputIndex() override
    {
        return TableIndex_;
    }

private:
    std::vector<TRowVtable> RowVtables_;

    NYT::TTableReaderPtr<TKVProto> TableReader_;
    std::vector<IRawCoderPtr> KeyDecoders_;
    std::vector<IRawCoderPtr> ValueDecoders_;
    std::vector<TRawRowHolder> RowHolders_;

    ssize_t TableIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

IYtNotSerializableJobInputPtr CreateSplitKvJobProtoInput(const std::vector<TRowVtable>& rowVtables, NYT::TTableReaderPtr<TKVProto> tableReader)
{
    return ::MakeIntrusive<TSplitKvJobProtoInput>(rowVtables, std::move(tableReader));
}

////////////////////////////////////////////////////////////////////////////////

template <class TMessage>
    requires std::derived_from<TMessage, ::google::protobuf::Message>
NYT::TTableRangesReaderPtr<TMessage> CreateRangesProtoTableReader(IInputStream* stream)
{
    auto impl = ::MakeIntrusive<NYT::TLenvalProtoTableReader>(
        ::MakeIntrusive<NYT::NDetail::TInputStreamProxy>(stream),
        TVector<const ::google::protobuf::Descriptor*>{TKVProto::GetDescriptor()}
    );
    return ::MakeIntrusive<NYT::TTableRangesReader<TMessage>>(impl);
}

////////////////////////////////////////////////////////////////////////////////

class TGbkImpulseReadProtoParDo
    : public IRawParDo
{
public:
    TGbkImpulseReadProtoParDo(IRawGroupByKeyPtr rawGroupByKey)
        : RawGroupByKey_(rawGroupByKey)
    { }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");

        Y_ABORT_UNLESS(outputs.size() == 1);
        Processed_ = false;
        Output_ = outputs[0];
    }

    void Do(const void* rows, int count) override
    {
        Y_ABORT_UNLESS(count == 1);
        Y_ABORT_UNLESS(*static_cast<const int*>(rows) == 0);
        Y_ABORT_UNLESS(!Processed_);
        Processed_ = true;

        const auto gbkInputTags = RawGroupByKey_->GetInputTags();
        Y_ABORT_UNLESS(gbkInputTags.size() == 1);
        const auto& rowVtable = gbkInputTags[0].GetRowVtable();

        auto rangesReader = CreateRangesProtoTableReader<TKVProto>(&Cin);

        for (; rangesReader->IsValid(); rangesReader->Next()) {
            auto range = &rangesReader->GetRange();
            auto input = CreateSplitKvJobProtoInput(std::vector{rowVtable}, range);
            RawGroupByKey_->ProcessOneGroup(input, Output_);
        }
    }

    void Finish() override
    { }

    const TFnAttributes& GetFnAttributes() const override
    {
        static TFnAttributes attributes;
        return attributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TGbkImpulseReadProtoParDo>(nullptr);
        };
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {
            {"input", MakeRowVtable<int>()}
        };
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return RawGroupByKey_->GetOutputTags();
    }

private:
    IRawGroupByKeyPtr RawGroupByKey_;

    Y_SAVELOAD_DEFINE_OVERRIDE(RawGroupByKey_);

    IRawOutputPtr Output_;
    bool Processed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateGbkImpulseReadProtoParDo(IRawGroupByKeyPtr rawComputation)
{
    return ::MakeIntrusive<TGbkImpulseReadProtoParDo>(std::move(rawComputation));
}

////////////////////////////////////////////////////////////////////////////////

class TCoGbkImpulseReadProtoParDo
    : public IRawParDo
{
public:
    TCoGbkImpulseReadProtoParDo() = default;

    TCoGbkImpulseReadProtoParDo(IRawCoGroupByKeyPtr rawCoGbk, std::vector<TRowVtable> rowVtable)
        : RawCoGroupByKey_(rawCoGbk)
        , InputRowVtableList_(rowVtable)
    { }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");

        Y_ABORT_UNLESS(outputs.size() == 1);
        Processed_ = false;
        Output_ = outputs[0];
    }

    void Do(const void* rows, int count) override
    {
        Y_ABORT_UNLESS(count == 1);
        Y_ABORT_UNLESS(*static_cast<const int*>(rows) == 0);
        Y_ABORT_UNLESS(!Processed_);
        Processed_ = true;


        auto rangesReader = CreateRangesProtoTableReader<TKVProto>(&Cin);

        for (; rangesReader->IsValid(); rangesReader->Next()) {
            auto range = &rangesReader->GetRange();
            auto input = CreateSplitKvJobProtoInput(InputRowVtableList_, range);
            ProcessOneGroup(RawCoGroupByKey_, input, Output_);
        }
    }

    void Finish() override
    { }

    const TFnAttributes& GetFnAttributes() const override
    {
        static TFnAttributes attributes;
        return attributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TCoGbkImpulseReadProtoParDo>();
        };
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {
            {"input", MakeRowVtable<int>()}
        };
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return RawCoGroupByKey_->GetOutputTags();
    }

private:
    IRawCoGroupByKeyPtr RawCoGroupByKey_;
    std::vector<TRowVtable> InputRowVtableList_;

    Y_SAVELOAD_DEFINE_OVERRIDE(RawCoGroupByKey_, InputRowVtableList_);

    IRawOutputPtr Output_;
    bool Processed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateCoGbkImpulseReadProtoParDo(
    IRawCoGroupByKeyPtr rawCoGbk,
    std::vector<TRowVtable> rowVtable)
{
    return ::MakeIntrusive<TCoGbkImpulseReadProtoParDo>(std::move(rawCoGbk), std::move(rowVtable));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
