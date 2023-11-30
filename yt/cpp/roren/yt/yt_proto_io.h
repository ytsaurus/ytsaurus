#pragma once

#include <yt/cpp/roren/yt/proto/kv.pb.h>
#include <yt/cpp/roren/yt/yt_io_private.h>

#include <yt/cpp/mapreduce/io/job_writer.h>
#include <yt/cpp/mapreduce/io/proto_table_reader.h>
#include <yt/cpp/mapreduce/io/proto_table_writer.h>
#include <yt/cpp/mapreduce/io/stream_table_reader.h>

namespace NRoren::NPrivate {

/////////////////////////////////////////////////////////////////////////////////

class IProtoIOParDo
    : public IRawParDo
{
public:
    virtual void SetTableIndex(ssize_t)
    {
        Y_ABORT("SetTableIndex is not implemented");
    }
};

using IProtoIOParDoPtr = ::TIntrusivePtr<IProtoIOParDo>;

///////////////////////////////////////////////////////////////////////////////

const TTypeTag<IRawParDoPtr> DecodingParDoTag("decoding_pardo");
const TTypeTag<IProtoIOParDoPtr> WriteParDoTag("write_pardo");
const TTypeTag<IRawParDoPtr> EncodingParDoTag("encoding_pardo");
const TTypeTag<const ::google::protobuf::Descriptor*> ProtoDescriptorTag("proto_descriptor");

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateReadProtoImpulseParDo(std::vector<TRowVtable>&& vtables);

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
    TWriteProtoParDo(ssize_t tableIndex = -1)
        : Descriptor_(TMessage::GetDescriptor())
        , TableIndex_(tableIndex)
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
            MakeHolder<::NYT::TSingleStreamJobWriter>(TableIndex_),
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
    const ::google::protobuf::Descriptor* Descriptor_;
    std::unique_ptr<::NYT::TLenvalProtoTableWriter> Writer_;

    ssize_t TableIndex_ = 0;

    Y_SAVELOAD_DEFINE_OVERRIDE(TableIndex_);
};

template <class TMessage>
IProtoIOParDoPtr CreateWriteProtoParDo(ssize_t tableIndex = -1)
{
    return ::MakeIntrusive<TWriteProtoParDo<TMessage>>(tableIndex);
}

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
            CreateWriteProtoParDo<TMessage>()
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

IRawParDoPtr CreateDecodingValueProtoParDo(TRowVtable rowVtable);

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateEncodingValueProtoParDo(TRowVtable rowVtable);

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateDecodingKeyValueProtoParDo(TRowVtable rowVtable);

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateEncodingKeyValueProtoParDo(TRowVtable rowVtable);

////////////////////////////////////////////////////////////////////////////////

IYtNotSerializableJobInputPtr CreateSplitKvJobProtoInput(
    const std::vector<TRowVtable>& rowVtables, NYT::TTableReaderPtr<TKVProto> tableReader);

IRawParDoPtr CreateGbkImpulseReadProtoParDo(IRawGroupByKeyPtr rawComputation);

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateCoGbkImpulseReadProtoParDo(
    IRawCoGroupByKeyPtr rawCoGbk,
    std::vector<TRowVtable> rowVtable);

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateDecodingKeyValueProtoParDo(TRowVtable rowVtable);

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateEncodingKeyValueProtoParDo(TRowVtable rowVtable);

////////////////////////////////////////////////////////////////////////////////

IYtNotSerializableJobInputPtr CreateSplitKvJobProtoInput(
    const std::vector<TRowVtable>& rowVtables, NYT::TTableReaderPtr<TKVProto> tableReader);

IRawParDoPtr CreateGbkImpulseReadProtoParDo(IRawGroupByKeyPtr rawComputation);

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateCoGbkImpulseReadProtoParDo(
    IRawCoGroupByKeyPtr rawCoGbk,
    std::vector<TRowVtable> rowVtable);

////////////////////////////////////////////////////////////////////////////////

IRawParDoPtr CreateCombineCombinerImpulseReadProtoParDo(IRawCombinePtr rawCombine);

IRawParDoPtr CreateCombineReducerImpulseReadProtoParDo(IRawCombinePtr rawCombine);

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

template <class TMessage>
IRawYtWritePtr MakeYtProtoSortedWrite(NYT::TRichYPath /*path*/, NYT::TTableSchema /*tableSchema*/, std::vector<std::string> /*columns*/)
{
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
