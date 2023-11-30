#include "yt_proto_io.h"

#include "jobs.h"

#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/io/proto_table_reader.h>
#include <yt/cpp/mapreduce/io/stream_table_reader.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TReadProtoImpulseParDo
    : public IRawParDo
{
public:
    TReadProtoImpulseParDo() = default;

    TReadProtoImpulseParDo(std::vector<TRowVtable> vtables)
        : TableCount_(std::ssize(vtables))
        , Vtables_(std::move(vtables))
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TReadProtoImpulseParDo.Input", MakeRowVtable<int>())};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        std::vector<TDynamicTypeTag> result;
        for (ssize_t i = 0; i < TableCount_; ++i) {
            result.emplace_back("TReadProtoImpulseParDo.Output." + ToString(i), Vtables_[i]);
        }
        return result;
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");
        Y_ABORT_UNLESS(std::ssize(outputs) == TableCount_);
        Outputs_ = outputs;
        Processed_ = false;
    }

    void Do(const void* rows, int count) override
    {
        Y_ABORT_UNLESS(!Processed_);
        Processed_ = true;
        Y_ABORT_UNLESS(count == 1);
        Y_ABORT_UNLESS(*static_cast<const int*>(rows) == 0);

        auto reader = ::MakeIntrusive<NYT::TLenvalProtoTableReader>(
            ::MakeIntrusive<NYT::NDetail::TInputStreamProxy>(&Cin)
        );

        for (; reader->IsValid(); reader->Next()) {
            auto tableIndex = reader->GetTableIndex();
            Y_ABORT_UNLESS(tableIndex < TableCount_);

            TRawRowHolder holder(Vtables_[tableIndex]);
            reader->ReadRow(reinterpret_cast<::google::protobuf::Message*>(holder.GetData()));
            Outputs_[tableIndex]->AddRaw(holder.GetData(), 1);
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
            return ::MakeIntrusive<TReadProtoImpulseParDo>();
        };
    }

private:
    ssize_t TableCount_;
    std::vector<TRowVtable> Vtables_;

    std::vector<IRawOutputPtr> Outputs_;
    bool Processed_ = false;

    Y_SAVELOAD_DEFINE_OVERRIDE(TableCount_, Vtables_);
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateReadProtoImpulseParDo(std::vector<TRowVtable>&& vtables)
{
    return ::MakeIntrusive<TReadProtoImpulseParDo>(std::move(vtables));
}

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

class TKvProtoOutput
    : public TYtJobOutput
    , public IKvJobOutput
{
public:
    TKvProtoOutput() = default;

    TKvProtoOutput(int sinkIndex, const std::vector<TRowVtable>& rowVtables)
        : TYtJobOutput(sinkIndex)
        , RowVtables_(rowVtables)
    {
        KeyEncoders_.reserve(RowVtables_.size());
        ValueEncoders_.reserve(RowVtables_.size());

        for (const auto& rowVtable : RowVtables_) {
            Y_ABORT_UNLESS(IsKv(rowVtable));
            KeyEncoders_.emplace_back(rowVtable.KeyVtableFactory().RawCoderFactory());
            ValueEncoders_.emplace_back(rowVtable.ValueVtableFactory().RawCoderFactory());
        }
    }

    TKvProtoOutput(int sinkIndex, IRawCoderPtr keyCoder, IRawCoderPtr valueCoder)
        : TYtJobOutput(sinkIndex)
        , KeyEncoders_({keyCoder})
        , ValueEncoders_({valueCoder})
    { }

    void AddRawToTable(const void* rows, ssize_t count, ui64 tableIndex) override
    {
        Y_ASSERT(tableIndex < RowVtables_.size());
        Y_ASSERT(IsDefined(RowVtables_[tableIndex]));
        auto dataSize = RowVtables_[tableIndex].DataSize;
        auto* current = static_cast<const std::byte*>(rows);
        for (ssize_t i = 0; i < count; ++i, current += dataSize) {
            AddKvToTable(
                GetKeyOfKv(RowVtables_[tableIndex], current),
                GetValueOfKv(RowVtables_[tableIndex], current),
                tableIndex
            );
        }
    }

    void AddRaw(const void* row, ssize_t count) override
    {
        AddRawToTable(row, count, /*tableIndex*/ 0);
    }

    void AddKvToTable(const void* key, const void* value, ui64 tableIndex) override
    {
        if (!Writer_) {
            Writer_ = std::make_unique<::NYT::TLenvalProtoTableWriter>(
                MakeHolder<::NYT::TSingleStreamJobWriter>(GetSinkIndices()[0]),
                TVector{TKVProto::GetDescriptor()}
            );
            for (const auto& keyEncoder : KeyEncoders_) {
                Y_ABORT_UNLESS(keyEncoder);
            }
            for (const auto& valueEncoder : ValueEncoders_) {
                Y_ABORT_UNLESS(valueEncoder);
            }
        }

        Key_.clear();
        Value_.clear();

        TStringOutput keyStream(Key_);
        KeyEncoders_[tableIndex]->EncodeRow(&keyStream, key);

        TStringOutput valueStream(Value_);
        ValueEncoders_[tableIndex]->EncodeRow(&valueStream, value);

        TKVProto msg;
        msg.SetKey(Key_);
        msg.SetValue(Value_);
        msg.SetTableIndex(tableIndex);

        Writer_->AddRow(msg, GetSinkIndices()[0]);
    }

    void Close() override
    { }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobOutputPtr {
            return ::MakeIntrusive<TKvProtoOutput>();
        };
    }

    void Save(IOutputStream* stream) const override
    {
        TYtJobOutput::Save(stream);

        SaveSize(stream, RowVtables_.size());
        SaveArray<TRowVtable>(stream, RowVtables_.data(), RowVtables_.size());

        SaveSize(stream, KeyEncoders_.size());
        for (const auto& keyEncoder : KeyEncoders_) {
            SaveSerializable(stream, keyEncoder);
        }

        SaveSize(stream, ValueEncoders_.size());
        for (const auto& valueEncoder : ValueEncoders_) {
            SaveSerializable(stream, valueEncoder);
        }
    }

    void Load(IInputStream* stream) override
    {
        TYtJobOutput::Load(stream);

        LoadSizeAndResize(stream, RowVtables_);
        LoadArray<TRowVtable>(stream, RowVtables_.data(), RowVtables_.size());

        size_t count = LoadSize(stream);
        KeyEncoders_.resize(count);
        for (auto& keyEncoder : KeyEncoders_) {
            LoadSerializable(stream, keyEncoder);
        }

        count = LoadSize(stream);
        ValueEncoders_.resize(count);
        for (auto& valueEncoder : ValueEncoders_) {
            LoadSerializable(stream, valueEncoder);
        }
    }

private:
    std::unique_ptr<::NYT::TLenvalProtoTableWriter> Writer_;
    std::vector<TRowVtable> RowVtables_;
    std::vector<IRawCoderPtr> KeyEncoders_;
    std::vector<IRawCoderPtr> ValueEncoders_;

    TString Key_;
    TString Value_;
};

////////////////////////////////////////////////////////////////////////////////

IKvJobOutputPtr CreateKvJobProtoOutput(int sinkIndex, IRawCoderPtr keyCoder, IRawCoderPtr valueCoder)
{
    return ::MakeIntrusive<TKvProtoOutput>(sinkIndex, std::move(keyCoder), std::move(valueCoder));
}

////////////////////////////////////////////////////////////////////////////////

template <class TMessage>
    requires std::derived_from<TMessage, ::google::protobuf::Message>
NYT::TTableRangesReaderPtr<TMessage> CreateRangesProtoTableReader(IInputStream* stream)
{
    auto impl = NYT::NDetail::CreateProtoReader(
        stream,
        {},
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

    IRawOutputPtr Output_;
    bool Processed_ = false;

    Y_SAVELOAD_DEFINE_OVERRIDE(RawCoGroupByKey_, InputRowVtableList_);
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateCoGbkImpulseReadProtoParDo(
    IRawCoGroupByKeyPtr rawCoGbk,
    std::vector<TRowVtable> rowVtable)
{
    return ::MakeIntrusive<TCoGbkImpulseReadProtoParDo>(std::move(rawCoGbk), std::move(rowVtable));
}

////////////////////////////////////////////////////////////////////////////////

class TCombineCombinerImpulseReadProtoParDo
    : public IRawParDo
{
public:
    TCombineCombinerImpulseReadProtoParDo() = default;

    explicit TCombineCombinerImpulseReadProtoParDo(IRawCombinePtr rawCombine)
        : RawCombine_(std::move(rawCombine))
    { }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");

        Y_ABORT_UNLESS(outputs.size() == 0);
        Processed_ = false;
    }

    void Do(const void* rows, int count) override
    {
        Y_ABORT_UNLESS(count == 1);
        Y_ABORT_UNLESS(*static_cast<const int*>(rows) == 0);
        Y_ABORT_UNLESS(!Processed_);
        Processed_ = true;

        auto rangesReader = CreateRangesProtoTableReader<TKVProto>(&Cin);

        auto accumVtable = RawCombine_->GetAccumVtable();
        auto inputVtable = RawCombine_->GetInputVtable();
        auto kvOutput = CreateKvJobProtoOutput(
            /*sinkIndex*/ 0,
            inputVtable.KeyVtableFactory().RawCoderFactory(),
            accumVtable.RawCoderFactory());

        TRawRowHolder accum(accumVtable);
        TRawRowHolder currentKey(inputVtable.KeyVtableFactory());
        for (; rangesReader->IsValid(); rangesReader->Next()) {
            auto range = &rangesReader->GetRange();
            auto input = CreateSplitKvJobProtoInput(std::vector{inputVtable}, range);

            bool first = true;
            RawCombine_->CreateAccumulator(accum.GetData());
            while (const auto* row = input->NextRaw()) {
                const auto* key = GetKeyOfKv(inputVtable, row);
                const auto* value = GetValueOfKv(inputVtable, row);

                RawCombine_->AddInput(accum.GetData(), value);

                if (first) {
                    first = false;
                    currentKey.CopyFrom(key);
                }
            }
            kvOutput->AddKvToTable(currentKey.GetData(), accum.GetData(), /*tableIndex*/ 0);
        }

        kvOutput->Close();
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
            return ::MakeIntrusive<TCombineCombinerImpulseReadProtoParDo>();
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
        return {};
    }

private:
    IRawCombinePtr RawCombine_;

    bool Processed_ = false;

    Y_SAVELOAD_DEFINE_OVERRIDE(RawCombine_);
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateCombineCombinerImpulseReadProtoParDo(IRawCombinePtr rawCombine)
{
    return ::MakeIntrusive<TCombineCombinerImpulseReadProtoParDo>(std::move(rawCombine));
}

////////////////////////////////////////////////////////////////////////////////

class TCombineReducerImpulseReadProtoParDo
    : public IRawParDo
{
public:
    TCombineReducerImpulseReadProtoParDo() = default;

    explicit TCombineReducerImpulseReadProtoParDo(IRawCombinePtr rawCombine)
        : RawCombine_(std::move(rawCombine))
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

        auto outRowVtable = RawCombine_->GetOutputVtable();
        auto accumVtable = RawCombine_->GetAccumVtable();
        auto keyVtable = outRowVtable.KeyVtableFactory();

        auto rangesReader = CreateRangesProtoTableReader<TKVProto>(&Cin);

        TRawRowHolder accum(accumVtable);
        TRawRowHolder currentKey(outRowVtable.KeyVtableFactory());
        TRawRowHolder out(outRowVtable);
        RawCombine_->CreateAccumulator(accum.GetData());
        for (; rangesReader->IsValid(); rangesReader->Next()) {
            auto range = &rangesReader->GetRange();
            auto keySavingInput =
                ::MakeIntrusive<TKeySavingInput>(keyVtable, accumVtable, range);
            keySavingInput->SaveNextKeyTo(out.GetKeyOfKV());
            RawCombine_->MergeAccumulators(accum.GetData(), keySavingInput);
            RawCombine_->ExtractOutput(out.GetValueOfKV(), accum.GetData());

            Output_->AddRaw(out.GetData(), 1);
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
            return ::MakeIntrusive<TCombineReducerImpulseReadProtoParDo>();
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
        return {
            {"TCombineReducerImpulseReadProtoParDo.Output", RawCombine_->GetOutputVtable()},
        };
    }

private:
    class TKeySavingInput
        : public IRawInput
    {
    public:
        TKeySavingInput(const TRowVtable& keyVtable, TRowVtable valueVtable, NYT::TTableReaderPtr<TKVProto> tableReader)
            : ValueVtable_(std::move(valueVtable))
            , TableReader_(std::move(tableReader))
            , KeyDecoder_(keyVtable.RawCoderFactory())
            , ValueDecoder_(ValueVtable_.RawCoderFactory())
            , ValueHolder_(ValueVtable_)
        { }

        void SaveNextKeyTo(void* key)
        {
            KeyOutput_ = key;
        }

        const void* NextRaw() override
        {
            if (TableReader_->IsValid()) {
                auto msg = TableReader_->GetRow();
                if (KeyOutput_) {
                    KeyDecoder_->DecodeRow(msg.GetKey(), KeyOutput_);
                    KeyOutput_ = nullptr;
                }
                ValueDecoder_->DecodeRow(msg.GetValue(), ValueHolder_.GetData());
                TableReader_->Next();
                return ValueHolder_.GetData();
            } else {
                return nullptr;
            }
        }

    private:
        TRowVtable ValueVtable_;

        NYT::TTableReaderPtr<TKVProto> TableReader_;
        IRawCoderPtr KeyDecoder_;
        IRawCoderPtr ValueDecoder_;
        TRawRowHolder ValueHolder_;

        void* KeyOutput_ = nullptr;
    };


private:
    IRawCombinePtr RawCombine_;

    IRawOutputPtr Output_;
    bool Processed_ = false;

    Y_SAVELOAD_DEFINE_OVERRIDE(RawCombine_);
};

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateCombineReducerImpulseReadProtoParDo(IRawCombinePtr rawCombine)
{
    return ::MakeIntrusive<TCombineReducerImpulseReadProtoParDo>(std::move(rawCombine));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
