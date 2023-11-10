#pragma once

#include <yt/cpp/roren/yt/proto/kv.pb.h>
#include <yt/cpp/roren/yt/yt_io_private.h>

#include <yt/cpp/mapreduce/io/job_writer.h>
#include <yt/cpp/mapreduce/io/proto_table_writer.h>

namespace NRoren::NPrivate {

/////////////////////////////////////////////////////////////////////////////////

class IProtoIOParDo
    : public IRawParDo
{
public:
    virtual void SetTableCount(ssize_t)
    {
        Y_ABORT("SetTableCount is not implemented");
    }
    virtual void SetTableIndex(ssize_t)
    {
        Y_ABORT("SetTableIndex is not implemented");
    }
};

using IProtoIOParDoPtr = ::TIntrusivePtr<IProtoIOParDo>;

///////////////////////////////////////////////////////////////////////////////

const TTypeTag<IProtoIOParDoPtr> ReadParDoTag("read_pardo");
const TTypeTag<IRawParDoPtr> DecodingParDoTag("decoding_pardo");
const TTypeTag<IProtoIOParDoPtr> WriteParDoTag("write_pardo");
const TTypeTag<IRawParDoPtr> EncodingParDoTag("encoding_pardo");
const TTypeTag<const ::google::protobuf::Descriptor*> ProtoDescriptorTag("proto_descriptor");

////////////////////////////////////////////////////////////////////////////////

template <class TMessage>
    requires std::derived_from<TMessage, ::google::protobuf::Message>
class TReadProtoImpulseParDo
    : public IProtoIOParDo
{
public:
    TReadProtoImpulseParDo() = default;

    void SetTableCount(ssize_t tableCount) override
    {
        TableCount_ = tableCount;
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TReadProtoImpulseParDo.Input", MakeRowVtable<int>())};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        std::vector<TDynamicTypeTag> result;
        for (ssize_t i = 0; i < TableCount_; ++i) {
            result.emplace_back("TReadProtoImpulseParDo.Output." + ToString(i), MakeRowVtable<TMessage>());
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

        auto reader = NYT::CreateTableReader<TMessage>(&Cin);

        for (; reader->IsValid(); reader->Next()) {
            auto tableIndex = reader->GetTableIndex();
            Y_ABORT_UNLESS(tableIndex < TableCount_);
            Outputs_[tableIndex]->AddRaw(&reader->GetRow(), 1);
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
    ssize_t TableCount_ = 0;

    std::vector<IRawOutputPtr> Outputs_;
    bool Processed_ = false;

    Y_SAVELOAD_DEFINE_OVERRIDE(TableCount_);
};

////////////////////////////////////////////////////////////////////////////////

template <class TMessage>
    requires std::derived_from<TMessage, ::google::protobuf::Message>
class TRawYtProtoRead
    : public IRawYtRead
{
public:
    explicit TRawYtProtoRead(NYT::TRichYPath path)
        : IRawYtRead(std::move(path))
    {
        NPrivate::SetAttribute(
            *this,
            ProtoDescriptorTag,
            TMessage::GetDescriptor()
        );
        NPrivate::SetAttribute(
            *this,
            DecodingParDoTag,
            MakeRawIdComputation(MakeRowVtable<TMessage>())
        );
        NPrivate::SetAttribute(
            *this,
            EncodingParDoTag,
            MakeRawIdComputation(MakeRowVtable<TMessage>())
        );
        NPrivate::SetAttribute(
            *this,
            ReadParDoTag,
            IProtoIOParDoPtr(::MakeIntrusive<TReadProtoImpulseParDo<TMessage>>())
        );
    }

    const void* NextRaw() override
    {
        // This input is not expected to be read for now.
        // The only use of it is to create job input.
        Y_ABORT("Not implemented");
    }

    IYtJobInputPtr CreateJobInput() const override
    {
        // Not supposed to be used since job input is depricated.
        Y_ABORT("Not implemented");
        return nullptr;
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag(TTypeTag<TMessage>("yt-proto-read-output-0"))};
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawReadPtr {
            return ::MakeIntrusive<TRawYtProtoRead<TMessage>>(NYT::TRichYPath{});
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TMessage>
    requires std::derived_from<TMessage, ::google::protobuf::Message>
class TWriteProtoParDo
    : public IProtoIOParDo
{
public:
    TWriteProtoParDo()
        : Descriptor_(TMessage::GetDescriptor())
    { }

    void SetTableIndex(ssize_t tableIndex) override
    {
        TableIndex_ = tableIndex;
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TWriteProtoParDo.Input", MakeRowVtable<TMessage>())};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {};
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");
        Y_ABORT_UNLESS(outputs.empty());

        Writer_ = std::make_unique<::NYT::TLenvalProtoTableWriter>(
            MakeHolder<::NYT::TJobWriter>(TableIndex_ + 1),
            TVector{Descriptor_}
        );
    }

    void Do(const void* rows, int count) override
    {
        const auto* current = static_cast<const TMessage*>(rows);
        for (ssize_t i = 0; i < count; ++i, ++current) {
            const auto& row = *static_cast<const TMessage*>(current);

            Writer_->AddRow(row, TableIndex_);
        }
    }

    void Finish() override
    {
        Writer_->FinishTable(TableIndex_);
    }

    const TFnAttributes& GetFnAttributes() const override
    {
        static const TFnAttributes fnAttributes;
        return fnAttributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TWriteProtoParDo>();
        };
    }

private:
    ssize_t TableIndex_ = 0;

    const ::google::protobuf::Descriptor* Descriptor_;
    std::unique_ptr<::NYT::TLenvalProtoTableWriter> Writer_;

    Y_SAVELOAD_DEFINE_OVERRIDE(TableIndex_);
};

////////////////////////////////////////////////////////////////////////////////

template <class TMessage>
    requires std::derived_from<TMessage, ::google::protobuf::Message>
class TRawYtProtoWrite
    : public IRawYtWrite
{
public:
    TRawYtProtoWrite(NYT::TRichYPath path, NYT::TTableSchema tableSchema)
        : IRawYtWrite(std::move(path), std::move(tableSchema))
    {
        NPrivate::SetAttribute(
            *this,
            ProtoDescriptorTag,
            TMessage::GetDescriptor()
        );
        NPrivate::SetAttribute(
            *this,
            DecodingParDoTag,
            MakeRawIdComputation(MakeRowVtable<TMessage>())
        );
        NPrivate::SetAttribute(
            *this,
            EncodingParDoTag,
            MakeRawIdComputation(MakeRowVtable<TMessage>())
        );
        NPrivate::SetAttribute(
            *this,
            WriteParDoTag,
            IProtoIOParDoPtr(::MakeIntrusive<TWriteProtoParDo<TMessage>>())
        );
    }

    IYtJobOutputPtr CreateJobOutput(int) const override
    {
        // Not supposed to be used since job output is depricated.
        Y_ABORT("Not implemented");
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag(TTypeTag<TMessage>("yt_proto_write_input_0"))};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {};
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawWritePtr {
            return ::MakeIntrusive<TRawYtProtoWrite<TMessage>>(NYT::TRichYPath{}, NYT::TTableSchema{});
        };
    }
};

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

IRawParDoPtr CreateDecodingValueProtoParDo(TRowVtable rowVtable);

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

IRawParDoPtr CreateEncodingValueProtoParDo(TRowVtable rowVtable);

////////////////////////////////////////////////////////////////////////////////

template <class TMessage>
IRawYtReadPtr MakeYtProtoRead(NYT::TRichYPath path)
{
    return ::MakeIntrusive<TRawYtProtoRead<TMessage>>(std::move(path));
}

template <class TMessage>
IRawYtWritePtr MakeYtProtoWrite(NYT::TRichYPath path, NYT::TTableSchema tableSchema)
{
    return ::MakeIntrusive<TRawYtProtoWrite<TMessage>>(std::move(path), std::move(tableSchema));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
