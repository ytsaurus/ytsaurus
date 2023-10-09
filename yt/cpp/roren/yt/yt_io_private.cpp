#include "yt_io_private.h"
#include "jobs.h"

#include <yt/cpp/roren/interface/private/raw_data_flow.h>
#include <yt/cpp/mapreduce/interface/format.h>
#include <yt/cpp/mapreduce/interface/io.h>
#include <yt/cpp/mapreduce/io/node_table_reader.h>
#include <yt/cpp/mapreduce/io/stream_table_reader.h>

#include <library/cpp/yson/writer.h>
#include <library/cpp/yson/node/node_visitor.h>

#include <util/stream/file.h>
#include <util/system/file.h>

namespace NRoren::NPrivate {

using NYT::TNode;

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void SaveViaNode(IOutputStream* output, const T& value)
{
    using namespace NYT;
    TNode node;
    TNodeBuilder nodeBuilder(&node);
    Serialize(value, &nodeBuilder);

    Save(output, node);
}

template <typename T>
void LoadViaNode(IInputStream* input, T& value)
{
    using namespace NYT;

    TNode node;
    Load(input, node);
    Deserialize(value, node);
}

////////////////////////////////////////////////////////////////////////////////

TYtJobOutput::TYtJobOutput(int sinkIndex)
    : SinkIndex_(sinkIndex)
{ }

int TYtJobOutput::GetSinkCount() const
{
    return 1;
}

std::vector<int> TYtJobOutput::GetSinkIndices() const
{
    return {SinkIndex_};
}

void TYtJobOutput::SetSinkIndices(const std::vector<int>& sinkIndices)
{
    Y_ABORT_UNLESS(sinkIndices.size() == 1);
    SinkIndex_ = std::move(sinkIndices[0]);
}

void TYtJobOutput::Save(IOutputStream* stream) const
{
    ::Save(stream, SinkIndex_);
}

void TYtJobOutput::Load(IInputStream* stream)
{
    ::Load(stream, SinkIndex_);
}

////////////////////////////////////////////////////////////////////////////////

class TYtJobNodeInput
    : public IYtJobInput
{
public:
    TYtJobNodeInput() = default;

    const void* NextRaw() override
    {
        if (Reader_ == nullptr) {
            // First row.
            Reader_ = NYT::CreateTableReader<NYT::TNode>(&Cin);
        } else if (Reader_->IsValid()) {
            Reader_->Next();
        }

        if (Reader_->IsValid()) {
            TableIndex_ = Reader_->GetTableIndex();
            return &Reader_->GetRow();
        } else {
            Reader_ = nullptr;
            return nullptr;
        }
    }

    ui64 GetInputIndex() override
    {
        return TableIndex_;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobInputPtr {
            return ::MakeIntrusive<TYtJobNodeInput>();
        };
    }

    void Save(IOutputStream* /*stream*/) const override
    { }

    void Load(IInputStream* /*stream*/) override
    { }

private:
    NYT::TTableReaderPtr<NYT::TNode> Reader_ = nullptr;
    ui64 TableIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TYtJobNodeOutput
    : public TYtJobOutput
{
public:
    TYtJobNodeOutput() = default;

    explicit TYtJobNodeOutput(int sinkIndex)
        : TYtJobOutput(sinkIndex)
    { }

    void AddRaw(const void* rows, ssize_t count) override
    {
        if (Writer_ == nullptr) {
            auto fd = GetOutputFD(GetSinkIndices()[0]);
            Stream_ = std::make_unique<TFileOutput>(Duplicate(fd));
            Writer_ = std::make_unique<::NYson::TYsonWriter>(
                Stream_.get(),
                NYson::EYsonFormat::Binary,
                ::NYson::EYsonType::ListFragment);
        }

        const auto* current = static_cast<const TNode*>(rows);
        for (ssize_t i = 0; i < count; ++i, current += sizeof(TNode)) {
            const NYT::TNode& row = *static_cast<const NYT::TNode*>(current);
            NYT::TNodeVisitor visitor(Writer_.get());
            visitor.Visit(row);
        }
    }

    void Close() override
    { }

    [[nodiscard]] TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobOutputPtr {
            return ::MakeIntrusive<TYtJobNodeOutput>();
        };
    }

private:
    std::unique_ptr<NYT::NYson::IYsonConsumer> Writer_ = nullptr;
    std::unique_ptr<IOutputStream> Stream_;
};

////////////////////////////////////////////////////////////////////////////////

class TRawYtNodeInput
    : public IRawYtRead
{
public:
    explicit TRawYtNodeInput(NYT::TRichYPath path)
        : IRawYtRead(std::move(path))
    { }

    const void* NextRaw() override
    {
        // This input is not expected to be read for now.
        // The only use of it is to create job input.
        Y_FAIL("Not implemented");
    }

    IYtJobInputPtr CreateJobInput() const override
    {
        return ::MakeIntrusive<TYtJobNodeInput>();
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag("foo", MakeRowVtable<NYT::TNode>())};
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawReadPtr {
            return ::MakeIntrusive<TRawYtNodeInput>(NYT::TRichYPath{});
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

IRawYtReadPtr MakeYtNodeInput(NYT::TRichYPath path)
{
    return ::MakeIntrusive<TRawYtNodeInput>(std::move(path));
}

////////////////////////////////////////////////////////////////////////////////

class TRawYtNodeWrite
    : public IRawYtWrite
{
public:
    TRawYtNodeWrite(NYT::TRichYPath path, NYT::TTableSchema tableSchema)
        : IRawYtWrite(std::move(path), std::move(tableSchema))
    { }

    IYtJobOutputPtr CreateJobOutput(int sinkIndex) const override
    {
        return ::MakeIntrusive<TYtJobNodeOutput>(sinkIndex);
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return std::vector<TDynamicTypeTag>{TTypeTag<NYT::TNode>("yt-node-write-input-0")};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {};
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawWritePtr {
            return ::MakeIntrusive<TRawYtNodeWrite>(NYT::TRichYPath{}, NYT::TTableSchema{});
        };
    }
};

IRawYtWritePtr MakeYtNodeWrite(NYT::TRichYPath path, NYT::TTableSchema tableSchema)
{
    return ::MakeIntrusive<TRawYtNodeWrite>(std::move(path), std::move(tableSchema));
}

////////////////////////////////////////////////////////////////////////////////

class TDecodingJobInput
    : public IYtJobInput
{
public:
    explicit TDecodingJobInput(std::vector<TRowVtable> rowVtables)
        : RowVtables_(std::move(rowVtables))
    { }

    const void* NextRaw() override
    {
        if (!NodeReader_) {
            NodeReader_ = NYT::CreateTableReader<TNode>(&Cin);

            Decoders_.reserve(RowVtables_.size());
            RowHolders_.reserve(RowVtables_.size());

            for (const auto& rowVtable : RowVtables_) {
                Decoders_.emplace_back(rowVtable.RawCoderFactory());
                RowHolders_.emplace_back(rowVtable);
            }
        }
        if (NodeReader_->IsValid()) {
            auto node = NodeReader_->GetRow();
            TableIndex_ = NodeReader_->GetTableIndex();
            auto& rowHolder = RowHolders_[TableIndex_];

            Decoders_[TableIndex_]->DecodeRow(node["value"].AsString(), rowHolder.GetData());
            NodeReader_->Next();
            return rowHolder.GetData();
        } else {
            return nullptr;
        }
    }

    ui64 GetInputIndex() override
    {
        return TableIndex_;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobInputPtr {
            return ::MakeIntrusive<TDecodingJobInput>(std::vector{TRowVtable()});
        };
    }

    void Save(IOutputStream* stream) const override
    {
        ::Save(stream, RowVtables_.size());
        for (const auto& rowVtable : RowVtables_) {
            ::Save(stream, rowVtable);
        }
    }

    void Load(IInputStream* stream) override
    {
        size_t count;
        ::Load(stream, count);
        RowVtables_.resize(count);
        for (auto& rowVtable : RowVtables_) {
            ::Load(stream, rowVtable);
        }
    }

private:
    std::vector<TRowVtable> RowVtables_;

    NYT::TTableReaderPtr<TNode> NodeReader_ = nullptr;
    std::vector<IRawCoderPtr> Decoders_;
    std::vector<TRawRowHolder> RowHolders_;
    ui64 TableIndex_ = 0;
};
////////////////////////////////////////////////////////////////////////////////

class TEncodingJobOutput
    : public TYtJobOutput
{
public:
    TEncodingJobOutput() = default;

    TEncodingJobOutput(const TRowVtable& rowVtable, int sinkIndex)
        : TYtJobOutput(sinkIndex)
        , Encoder_(rowVtable.RawCoderFactory())
        , DataSize_(rowVtable.DataSize)
    { }

    void AddRaw(const void* rows, ssize_t count) override
    {
        auto* current = static_cast<const std::byte*>(rows);
        for (ssize_t i = 0; i < count; ++i, current += DataSize_) {
            if (!Writer_) {
                auto fd = GetOutputFD(GetSinkIndices()[0]);
                Stream_ = std::make_unique<TFileOutput>(Duplicate(fd));
                Writer_ = std::make_unique<::NYson::TYsonWriter>(
                    Stream_.get(),
                    NYT::NYson::EYsonFormat::Binary,
                    ::NYson::EYsonType::ListFragment
                );
                Y_ABORT_UNLESS(Encoder_);
            }
            Value_.clear();
            TStringOutput str(Value_);
            Encoder_->EncodeRow(&str, current);

            Writer_->OnListItem();
            Writer_->OnBeginMap();
            Writer_->OnKeyedItem("value");
            Writer_->OnStringScalar(Value_);
            Writer_->OnEndMap();
        }
    }

    void Close() override
    { }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobOutputPtr {
            return ::MakeIntrusive<TEncodingJobOutput>();
        };
    }

    void Save(IOutputStream* stream) const override
    {
        TYtJobOutput::Save(stream);
        SaveSerializable(stream, Encoder_);
        ::Save(stream, DataSize_);
    }

    void Load(IInputStream* stream) override
    {
        TYtJobOutput::Load(stream);
        LoadSerializable(stream, Encoder_);
        ::Load(stream, DataSize_);
    }

private:
    std::unique_ptr<NYT::NYson::IYsonConsumer> Writer_;
    std::unique_ptr<IOutputStream> Stream_;
    IRawCoderPtr Encoder_;
    size_t DataSize_ = 0;
    TString Value_;
};

////////////////////////////////////////////////////////////////////////////////

class TKvOutput
    : public TYtJobOutput
    , public IKvJobOutput
{
public:
    TKvOutput() = default;

    TKvOutput(int sinkIndex, const std::vector<TRowVtable>& rowVtables)
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

    TKvOutput(int sinkIndex, IRawCoderPtr keyCoder, IRawCoderPtr valueCoder)
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
            auto fd = GetOutputFD(GetSinkIndices()[0]);
            Stream_ = std::make_unique<TFileOutput>(Duplicate(fd));
            Writer_ = std::make_unique<::NYson::TYsonWriter>(
                Stream_.get(),
                NYT::NYson::EYsonFormat::Binary,
                ::NYson::EYsonType::ListFragment
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

        Writer_->OnListItem();
        Writer_->OnBeginMap();
        Writer_->OnKeyedItem("key");
        Writer_->OnStringScalar(Key_);
        Writer_->OnKeyedItem("value");
        Writer_->OnStringScalar(Value_);
        Writer_->OnKeyedItem("table_index");
        Writer_->OnInt64Scalar(tableIndex);
        Writer_->OnEndMap();
    }

    void Close() override
    { }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobOutputPtr {
            return ::MakeIntrusive<TKvOutput>();
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
    std::unique_ptr<IOutputStream> Stream_;
    std::unique_ptr<NYT::NYson::IYsonConsumer> Writer_;
    std::vector<TRowVtable> RowVtables_;
    std::vector<IRawCoderPtr> KeyEncoders_;
    std::vector<IRawCoderPtr> ValueEncoders_;

    TString Key_;
    TString Value_;
};

////////////////////////////////////////////////////////////////////////////////

class TSplitKvJobInput
    : public IYtNotSerializableJobInput
{
public:
    TSplitKvJobInput() = default;

    explicit TSplitKvJobInput(const std::vector<TRowVtable>& rowVtables, NYT::TTableReaderPtr<TNode> tableReader)
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
            auto node = TableReader_->GetRow();
            if (node.HasKey("table_index")) {
                TableIndex_ = node["table_index"].AsInt64();
            }
            Y_ENSURE(TableIndex_ < RowVtables_.size());

            auto& rowHolder = RowHolders_[TableIndex_];

            KeyDecoders_[TableIndex_]->DecodeRow(node["key"].AsString(), rowHolder.GetKeyOfKV());
            ValueDecoders_[TableIndex_]->DecodeRow(node["value"].AsString(), rowHolder.GetValueOfKV());

            TableReader_->Next();
            return rowHolder.GetData();
        } else {
            return nullptr;
        }
    }

    ui64 GetInputIndex() override
    {
        return TableIndex_;
    }

private:
    std::vector<TRowVtable> RowVtables_;

    NYT::TTableReaderPtr<TNode> TableReader_;
    std::vector<IRawCoderPtr> KeyDecoders_;
    std::vector<IRawCoderPtr> ValueDecoders_;
    std::vector<TRawRowHolder> RowHolders_;

    ui64 TableIndex_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TTeeJobOutput
    : public IYtJobOutput
{
public:
    TTeeJobOutput() = default;

    TTeeJobOutput(std::vector<IYtJobOutputPtr> outputs)
        : Outputs_(std::move(outputs))
    { }

    void AddRaw(const void* row, ssize_t count) override
    {
        Y_ABORT_UNLESS(!Outputs_.empty());
        for (const auto& output : Outputs_) {
            output->AddRaw(row, count);
        }
    }

    void Close() override
    {
        for (const auto& output : Outputs_) {
            output->Close();
        }
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobOutputPtr {
            return ::MakeIntrusive<TTeeJobOutput>();
        };
    }

    void Save(IOutputStream* stream) const override
    {
        ::Save(stream, std::ssize(Outputs_));
        for (const auto& output : Outputs_) {
            SaveSerializable(stream, output);
        }
    }

    void Load(IInputStream* stream) override
    {
        ssize_t size;
        ::Load(stream, size);
        Outputs_.resize(size);
        for (int i = 0; i < size; ++i) {
            LoadSerializable(stream, Outputs_[i]);
        }
    }

    int GetSinkCount() const override
    {
        int count = 0;
        for (const auto& output : Outputs_) {
            count += output->GetSinkCount();
        }
        return count;
    }

    std::vector<int> GetSinkIndices() const override
    {
        std::vector<int> result;
        result.reserve(GetSinkCount());
        for (const auto& output : Outputs_) {
            auto indices = output->GetSinkIndices();
            result.insert(result.end(), indices.begin(), indices.end());
        }
        return result;
    }

    void SetSinkIndices(const std::vector<int>& sinkIndices) override
    {
        auto it = sinkIndices.begin();
        for (const auto& output : Outputs_) {
            auto count = output->GetSinkCount();
            output->SetSinkIndices(std::vector<int>(it, it + count));
            it += count;
        }
        Y_ABORT_UNLESS(it == sinkIndices.end());
    }

protected:
    std::vector<IYtJobOutputPtr> Outputs_;
};

////////////////////////////////////////////////////////////////////////////////

class TParDoJobOutput
    : public TTeeJobOutput
{
public:
    TParDoJobOutput() = default;

    TParDoJobOutput(IRawParDoPtr rawParDo, std::vector<IYtJobOutputPtr> outputs)
        : TTeeJobOutput(std::move(outputs))
        , RawParDo_(std::move(rawParDo))
    { }

    void AddRaw(const void* row, ssize_t count) override
    {
        if (!Started_) {
            std::vector<IRawOutputPtr> outputs(Outputs_.begin(), Outputs_.end());
            auto executionContext = CreateYtExecutionContext();
            RawParDo_->Start(executionContext, outputs);
            Started_ = true;
        }

        RawParDo_->Do(row, count);
    }

    void Close() override
    {
        RawParDo_->Finish();
        TTeeJobOutput::Close();
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobOutputPtr {
            return ::MakeIntrusive<TParDoJobOutput>();
        };
    }

    void Save(IOutputStream* stream) const override
    {
        TTeeJobOutput::Save(stream);
        SaveSerializable(stream, RawParDo_);
    }

    void Load(IInputStream* stream) override
    {
        TTeeJobOutput::Load(stream);
        LoadSerializable(stream, RawParDo_);
    }

private:
    IRawParDoPtr RawParDo_;
    bool Started_ = false;
};

////////////////////////////////////////////////////////////////////////////////

IYtJobInputPtr CreateDecodingJobInput(TRowVtable rowVtable)
{
    return CreateDecodingJobInput(std::vector{std::move(rowVtable)});
}

IYtJobInputPtr CreateDecodingJobInput(const std::vector<TRowVtable>& rowVtables)
{
    return ::MakeIntrusive<TDecodingJobInput>(rowVtables);
}

IYtNotSerializableJobInputPtr CreateSplitKvJobInput(const std::vector<TRowVtable>& rowVtables, NYT::TTableReaderPtr<TNode> tableReader)
{
    return ::MakeIntrusive<TSplitKvJobInput>(rowVtables, std::move(tableReader));
}

IYtJobOutputPtr CreateEncodingJobOutput(const TRowVtable& rowVtable, int sinkIndex)
{
    return ::MakeIntrusive<TEncodingJobOutput>(rowVtable, sinkIndex);
}

IKvJobOutputPtr CreateKvJobOutput(int sinkIndex, const std::vector<TRowVtable>& rowVtables)
{
    return ::MakeIntrusive<TKvOutput>(sinkIndex, rowVtables);
}

IKvJobOutputPtr CreateKvJobOutput(int sinkIndex, IRawCoderPtr keyCoder, IRawCoderPtr valueCoder)
{
    return ::MakeIntrusive<TKvOutput>(sinkIndex, std::move(keyCoder), std::move(valueCoder));
}

IYtJobOutputPtr CreateTeeJobOutput(std::vector<IYtJobOutputPtr> outputs)
{
    return ::MakeIntrusive<TTeeJobOutput>(std::move(outputs));
}

IYtJobOutputPtr CreateParDoJobOutput(IRawParDoPtr rawParDo, std::vector<IYtJobOutputPtr> outputs)
{
    return ::MakeIntrusive<TParDoJobOutput>(std::move(rawParDo), std::move(outputs));
}

////////////////////////////////////////////////////////////////////////////////

class TYtJobOutputParDo
    : public IRawParDo
{
public:
    TYtJobOutputParDo() = default;

    TYtJobOutputParDo(IYtJobOutputPtr jobOutput, TRowVtable rowVtable)
        : JobOutput_(std::move(jobOutput))
        , RowVtable_(std::move(rowVtable))
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TYtJobOutputParDo.Input", RowVtable_)};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {};
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");
        Y_ABORT_UNLESS(outputs.empty(), "Size of outputs: %ld", outputs.size());
    }

    void Do(const void* rows, int count) override
    {
        JobOutput_->AddRaw(rows, count);
    }

    void Finish() override
    {
        JobOutput_->Close();
    }

    const TFnAttributes& GetFnAttributes() const override
    {
        static const TFnAttributes FnAttributes_;
        return FnAttributes_;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TYtJobOutputParDo>();
        };
    }

private:
    IYtJobOutputPtr JobOutput_;
    TRowVtable RowVtable_;

    Y_SAVELOAD_DEFINE_OVERRIDE(JobOutput_, RowVtable_);
};

IRawParDoPtr CreateOutputParDo(IYtJobOutputPtr output, TRowVtable rowVtable)
{
    return ::MakeIntrusive<TYtJobOutputParDo>(std::move(output), std::move(rowVtable));
}

////////////////////////////////////////////////////////////////////////////////

class TDecodingValueNodeParDo
    : public IRawParDo
{
public:
    TDecodingValueNodeParDo() = default;

    explicit TDecodingValueNodeParDo(TRowVtable rowVtable)
        : OutputRowVtable_(rowVtable)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TDecodingValueNodeParDo.Input", MakeRowVtable<TNode>())};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag("TDecodingValueNodeParDo.Output", OutputRowVtable_)};
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
        const auto* curRow = static_cast<const TNode*>(rows);
        for (int i = 0; i < count; ++i, ++curRow) {
            Coder_->DecodeRow((*curRow)["value"].AsString(), OutputRowHolder_.GetData());
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
            return ::MakeIntrusive<TDecodingValueNodeParDo>();
        };
    }

private:
    TRowVtable OutputRowVtable_;

    IRawOutputPtr Output_;
    IRawCoderPtr Coder_;
    TRawRowHolder OutputRowHolder_;

    Y_SAVELOAD_DEFINE_OVERRIDE(OutputRowVtable_);
};


IRawParDoPtr CreateDecodingValueNodeParDo(TRowVtable rowVtable)
{
    return ::MakeIntrusive<TDecodingValueNodeParDo>(rowVtable);
}

////////////////////////////////////////////////////////////////////////////////

class TEncodingValueNodeParDo
    : public IRawParDo
{
public:
    TEncodingValueNodeParDo() = default;

    explicit TEncodingValueNodeParDo(TRowVtable rowVtable)
        : InputRowVtable_(rowVtable)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TEncodingValueNodeParDo.Input", InputRowVtable_)};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag("TEncodingValueNodeParDo.Output", MakeRowVtable<TNode>())};
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
            ResultNode_["value"] = Buffer_;
            Output_->AddRaw(&ResultNode_, 1);
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
            return ::MakeIntrusive<TEncodingValueNodeParDo>();
        };
    }

private:
    TRowVtable InputRowVtable_;

    IRawOutputPtr Output_;
    IRawCoderPtr Coder_;
    TNode ResultNode_;
    TString Buffer_;

    Y_SAVELOAD_DEFINE_OVERRIDE(InputRowVtable_);
};

IRawParDoPtr CreateEncodingValueNodeParDo(TRowVtable rowVtable)
{
    return ::MakeIntrusive<TEncodingValueNodeParDo>(rowVtable);
}

////////////////////////////////////////////////////////////////////////////////

class TDecodingKeyValueNodeParDo
    : public IRawParDo
{
public:
    TDecodingKeyValueNodeParDo() = default;

    explicit TDecodingKeyValueNodeParDo(TRowVtable rowVtable)
        : OutputRowVtable_(rowVtable)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TDecodingKeyValueNodeParDo.Input", MakeRowVtable<TNode>())};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag("TDecodingKeyValueNodeParDo.Output", OutputRowVtable_)};
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
        const auto* curRow = static_cast<const TNode*>(rows);
        for (int i = 0; i < count; ++i, ++curRow) {
            KeyCoder_->DecodeRow((*curRow)["key"].AsString(), OutputRowHolder_.GetKeyOfKV());
            ValueCoder_->DecodeRow((*curRow)["value"].AsString(), OutputRowHolder_.GetValueOfKV());
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
            return ::MakeIntrusive<TDecodingKeyValueNodeParDo>();
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

IRawParDoPtr CreateDecodingKeyValueNodeParDo(TRowVtable rowVtable)
{
    return ::MakeIntrusive<TDecodingKeyValueNodeParDo>(rowVtable);
}

////////////////////////////////////////////////////////////////////////////////

class TEncodingKeyValueNodeParDo
    : public IRawParDo
{
public:
    TEncodingKeyValueNodeParDo() = default;

    explicit TEncodingKeyValueNodeParDo(TRowVtable rowVtable)
        : InputRowVtable_(rowVtable)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TEncodingKeyValueNodeParDo.Input", InputRowVtable_)};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {TDynamicTypeTag("TEncodingKeyValueNodeParDo.Output", MakeRowVtable<TNode>())};
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
            ResultNode_["key"] = KeyBuffer_;
            ResultNode_["value"] = ValueBuffer_;
            Output_->AddRaw(&ResultNode_, 1);
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
            return ::MakeIntrusive<TEncodingKeyValueNodeParDo>();
        };
    }

private:
    TRowVtable InputRowVtable_;

    IRawOutputPtr Output_;
    IRawCoderPtr KeyCoder_;
    IRawCoderPtr ValueCoder_;
    TNode ResultNode_;
    TString KeyBuffer_;
    TString ValueBuffer_;

    Y_SAVELOAD_DEFINE_OVERRIDE(InputRowVtable_);
};

IRawParDoPtr CreateEncodingKeyValueNodeParDo(TRowVtable rowVtable)
{
    return ::MakeIntrusive<TEncodingKeyValueNodeParDo>(rowVtable);
}

////////////////////////////////////////////////////////////////////////////////

class TReadNodeImpulseParDo
    : public IRawParDo
{
public:
    TReadNodeImpulseParDo() = default;

    explicit TReadNodeImpulseParDo(ssize_t tableCount)
        : TableCount_(tableCount)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TReadNodeImpulseParDo.Input", MakeRowVtable<int>())};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        std::vector<TDynamicTypeTag> result;
        for (ssize_t i = 0; i < TableCount_; ++i) {
            result.emplace_back("TReadNodeImpulseParDo.Output." + ToString(i), MakeRowVtable<TNode>());
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

        auto reader = NYT::CreateTableReader<NYT::TNode>(&Cin);

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
            return ::MakeIntrusive<TReadNodeImpulseParDo>();
        };
    }

private:
    ssize_t TableCount_ = 0;

    std::vector<IRawOutputPtr> Outputs_;
    bool Processed_ = false;

    Y_SAVELOAD_DEFINE_OVERRIDE(TableCount_);
};

IRawParDoPtr CreateReadNodeImpulseParDo(ssize_t tableCount)
{
    return ::MakeIntrusive<TReadNodeImpulseParDo>(tableCount);
}

////////////////////////////////////////////////////////////////////////////////

class TWriteNodeParDo
    : public IRawParDo
{
public:
    TWriteNodeParDo() = default;

    explicit TWriteNodeParDo(ssize_t tableCount)
        : TableIndex_(tableCount)
    { }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return {TDynamicTypeTag("TWriteNodeParDo.Input", MakeRowVtable<TNode>())};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {};
    }

    void Start(const IExecutionContextPtr& context, const std::vector<IRawOutputPtr>& outputs) override
    {
        Y_ABORT_UNLESS(context->GetExecutorName() == "yt");
        Y_ABORT_UNLESS(outputs.empty());

        auto fd = GetOutputFD(TableIndex_);
        Stream_ = std::make_unique<TFileOutput>(Duplicate(fd));
        YsonWriter_ = std::make_unique<::NYson::TYsonWriter>(
            Stream_.get(),
            NYson::EYsonFormat::Binary,
            ::NYson::EYsonType::ListFragment);
    }

    void Do(const void* rows, int count) override
    {
        const auto* current = static_cast<const TNode*>(rows);
        for (ssize_t i = 0; i < count; ++i, ++current) {
            const NYT::TNode& row = *static_cast<const NYT::TNode*>(current);
            NYT::TNodeVisitor visitor(YsonWriter_.get());
            visitor.Visit(row);
        }
    }

    void Finish() override
    {
        YsonWriter_.reset();
        Stream_->Finish();
        Stream_.reset();
    }

    const TFnAttributes& GetFnAttributes() const override
    {
        static const TFnAttributes fnAttributes;
        return fnAttributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return ::MakeIntrusive<TWriteNodeParDo>();
        };
    }

private:
    ssize_t TableIndex_ = 0;

    std::unique_ptr<NYT::NYson::IYsonConsumer> YsonWriter_;
    std::unique_ptr<IOutputStream> Stream_;

    Y_SAVELOAD_DEFINE_OVERRIDE(TableIndex_);
};

IRawParDoPtr CreateWriteNodeParDo(ssize_t tableIndex)
{
    return ::MakeIntrusive<TWriteNodeParDo>(tableIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
