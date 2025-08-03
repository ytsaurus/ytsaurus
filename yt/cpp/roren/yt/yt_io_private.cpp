#include "yt_io_private.h"
#include "jobs.h"
#include "table_stream_registry.h"

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

    ssize_t GetInputIndex() override
    {
        return TableIndex_;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobInputPtr {
            return NYT::New<TYtJobNodeInput>();
        };
    }

    void Save(IOutputStream* /*stream*/) const override
    { }

    void Load(IInputStream* /*stream*/) override
    { }

private:
    NYT::TTableReaderPtr<NYT::TNode> Reader_ = nullptr;
    ssize_t TableIndex_ = 0;
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
            Stream_ = GetTableStream(GetSinkIndices()[0]);
            Writer_ = std::make_unique<::NYson::TYsonWriter>(
                Stream_,
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
    {
        Stream_->Flush();
    }

    [[nodiscard]] TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobOutputPtr {
            return NYT::New<TYtJobNodeOutput>();
        };
    }

private:
    std::unique_ptr<NYT::NYson::IYsonConsumer> Writer_ = nullptr;
    IOutputStream* Stream_ = nullptr;
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
        Y_ABORT("Not implemented");
    }

    IYtJobInputPtr CreateJobInput() const override
    {
        return NYT::New<TYtJobNodeInput>();
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
            return NYT::New<TRawYtNodeInput>(NYT::TRichYPath{});
        };
    }
};

////////////////////////////////////////////////////////////////////////////////

IRawYtReadPtr MakeYtNodeInput(NYT::TRichYPath path)
{
    return NYT::New<TRawYtNodeInput>(std::move(path));
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
        return NYT::New<TYtJobNodeOutput>(sinkIndex);
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
            return NYT::New<TRawYtNodeWrite>(NYT::TRichYPath{}, NYT::TTableSchema{});
        };
    }
};

IRawYtWritePtr MakeYtNodeWrite(NYT::TRichYPath path, NYT::TTableSchema tableSchema)
{
    return NYT::New<TRawYtNodeWrite>(std::move(path), std::move(tableSchema));
}

////////////////////////////////////////////////////////////////////////////////

void FillSchemaFromSortColumns(NYT::TTableSchema& schema, const NYT::TSortColumns& columnsToSort, bool uniqueKeys)
{
    for (auto& column : schema.MutableColumns()) {
        for (const auto& sortedColumn : columnsToSort.Parts_) {
            if (column.Name() == sortedColumn.Name()) {
                column.SortOrder(sortedColumn.SortOrder());
            }
        }
    }
    schema.UniqueKeys(uniqueKeys);
}

////////////////////////////////////////////////////////////////////////////////

class TRawYtNodeSortedWrite
    : public IRawYtSortedWrite
{
public:
    TRawYtNodeSortedWrite(NYT::TRichYPath path, NYT::TTableSchema tableSchema)
        : IRawYtSortedWrite(std::move(path), ToUnsortedSchema(tableSchema))
        , SortedSchema_(std::move(tableSchema))
    { }

    NYT::TSortColumns GetColumnsToSort() const override
    {
        return GetSortColumns(SortedSchema_);
    }

    NYT::TTableSchema GetSortedSchema() const override
    {
        return SortedSchema_;
    }

    IYtJobOutputPtr CreateJobOutput(int sinkIndex) const override
    {
        return NYT::New<TYtJobNodeOutput>(sinkIndex);
    }

    std::vector<TDynamicTypeTag> GetInputTags() const override
    {
        return std::vector<TDynamicTypeTag>{TTypeTag<NYT::TNode>("yt-node-sorted-write-input-0")};
    }

    std::vector<TDynamicTypeTag> GetOutputTags() const override
    {
        return {};
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawWritePtr {
            return NYT::New<TRawYtNodeSortedWrite>(
                NYT::TRichYPath{},
                NYT::TTableSchema{}
            );
        };
    }

    void Save(IOutputStream*) const override
    {
        Y_ABORT("TRawYtProtoSortedWrite object is not supposed to be SaveLoad-ed");
    }

    void Load(IInputStream*) override
    {
        Y_ABORT("TRawYtProtoSortedWrite object is not supposed to be SaveLoad-ed");
    }

private:
    NYT::TTableSchema SortedSchema_;
};

IRawYtSortedWritePtr MakeYtNodeSortedWrite(NYT::TRichYPath path, NYT::TTableSchema tableSchema)
{
    return NYT::New<TRawYtNodeSortedWrite>(std::move(path), std::move(tableSchema));
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

    ssize_t GetInputIndex() override
    {
        return TableIndex_;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobInputPtr {
            return NYT::New<TDecodingJobInput>(std::vector{TRowVtable()});
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
    ssize_t TableIndex_ = 0;
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
                Stream_ = GetTableStream(GetSinkIndices()[0]);
                Writer_ = std::make_unique<::NYson::TYsonWriter>(
                    Stream_,
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
    {
        Stream_->Flush();
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobOutputPtr {
            return NYT::New<TEncodingJobOutput>();
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
    IOutputStream* Stream_ = nullptr;
    IRawCoderPtr Encoder_;
    size_t DataSize_ = 0;
    TString Value_;
};

////////////////////////////////////////////////////////////////////////////////

class TKvNodeOutput
    : public TYtJobOutput
    , public IKvJobOutput
{
public:
    TKvNodeOutput() = default;

    TKvNodeOutput(int sinkIndex, const std::vector<TRowVtable>& rowVtables)
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

    TKvNodeOutput(int sinkIndex, IRawCoderPtr keyCoder, IRawCoderPtr valueCoder)
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
            Stream_ = GetTableStream(GetSinkIndices()[0]);
            Writer_ = std::make_unique<::NYson::TYsonWriter>(
                Stream_,
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
    {
        Stream_->Flush();
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IYtJobOutputPtr {
            return NYT::New<TKvNodeOutput>();
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
    IOutputStream* Stream_ = nullptr;
    std::unique_ptr<NYT::NYson::IYsonConsumer> Writer_;
    std::vector<TRowVtable> RowVtables_;
    std::vector<IRawCoderPtr> KeyEncoders_;
    std::vector<IRawCoderPtr> ValueEncoders_;

    TString Key_;
    TString Value_;
};

////////////////////////////////////////////////////////////////////////////////

class TSplitKvJobNodeInput
    : public IYtNotSerializableJobInput
{
public:
    TSplitKvJobNodeInput() = default;

    explicit TSplitKvJobNodeInput(const std::vector<TRowVtable>& rowVtables, NYT::TTableReaderPtr<TNode> tableReader)
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
            Y_ENSURE(TableIndex_ < ssize(RowVtables_));

            auto& rowHolder = RowHolders_[TableIndex_];

            KeyDecoders_[TableIndex_]->DecodeRow(node["key"].AsString(), rowHolder.GetKeyOfKV());
            ValueDecoders_[TableIndex_]->DecodeRow(node["value"].AsString(), rowHolder.GetValueOfKV());

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

    NYT::TTableReaderPtr<TNode> TableReader_;
    std::vector<IRawCoderPtr> KeyDecoders_;
    std::vector<IRawCoderPtr> ValueDecoders_;
    std::vector<TRawRowHolder> RowHolders_;

    ssize_t TableIndex_ = 0;
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
            return NYT::New<TTeeJobOutput>();
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
            return NYT::New<TParDoJobOutput>();
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
    return NYT::New<TDecodingJobInput>(rowVtables);
}

IYtNotSerializableJobInputPtr CreateSplitKvJobNodeInput(const std::vector<TRowVtable>& rowVtables, NYT::TTableReaderPtr<NYT::TNode> tableReader)
{
    return NYT::New<TSplitKvJobNodeInput>(rowVtables, std::move(tableReader));
}

IYtJobOutputPtr CreateEncodingJobOutput(const TRowVtable& rowVtable, int sinkIndex)
{
    return NYT::New<TEncodingJobOutput>(rowVtable, sinkIndex);
}

IKvJobOutputPtr CreateKvJobNodeOutput(int sinkIndex, const std::vector<TRowVtable>& rowVtables)
{
    return NYT::New<TKvNodeOutput>(sinkIndex, rowVtables);
}

IKvJobOutputPtr CreateKvJobNodeOutput(int sinkIndex, IRawCoderPtr keyCoder, IRawCoderPtr valueCoder)
{
    return NYT::New<TKvNodeOutput>(sinkIndex, std::move(keyCoder), std::move(valueCoder));
}

IYtJobOutputPtr CreateTeeJobOutput(std::vector<IYtJobOutputPtr> outputs)
{
    return NYT::New<TTeeJobOutput>(std::move(outputs));
}

IYtJobOutputPtr CreateParDoJobOutput(IRawParDoPtr rawParDo, std::vector<IYtJobOutputPtr> outputs)
{
    return NYT::New<TParDoJobOutput>(std::move(rawParDo), std::move(outputs));
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
            return NYT::New<TYtJobOutputParDo>();
        };
    }

private:
    IYtJobOutputPtr JobOutput_;
    TRowVtable RowVtable_;

    Y_SAVELOAD_DEFINE_OVERRIDE(JobOutput_, RowVtable_);
};

IRawParDoPtr CreateOutputParDo(IYtJobOutputPtr output, TRowVtable rowVtable)
{
    return NYT::New<TYtJobOutputParDo>(std::move(output), std::move(rowVtable));
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
            return NYT::New<TDecodingValueNodeParDo>();
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
    return NYT::New<TDecodingValueNodeParDo>(rowVtable);
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
            return NYT::New<TEncodingValueNodeParDo>();
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
    return NYT::New<TEncodingValueNodeParDo>(rowVtable);
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
            return NYT::New<TDecodingKeyValueNodeParDo>();
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
    return NYT::New<TDecodingKeyValueNodeParDo>(rowVtable);
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
            return NYT::New<TEncodingKeyValueNodeParDo>();
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
    return NYT::New<TEncodingKeyValueNodeParDo>(rowVtable);
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
            return NYT::New<TReadNodeImpulseParDo>();
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
    return NYT::New<TReadNodeImpulseParDo>(tableCount);
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

        Stream_ = GetTableStream(TableIndex_);
        YsonWriter_ = std::make_unique<::NYson::TYsonWriter>(
            Stream_,
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
        Stream_->Flush();
    }

    const TFnAttributes& GetFnAttributes() const override
    {
        static const TFnAttributes fnAttributes;
        return fnAttributes;
    }

    TDefaultFactoryFunc GetDefaultFactory() const override
    {
        return [] () -> IRawParDoPtr {
            return NYT::New<TWriteNodeParDo>();
        };
    }

private:
    ssize_t TableIndex_ = 0;

    std::unique_ptr<NYT::NYson::IYsonConsumer> YsonWriter_;
    IOutputStream* Stream_ = nullptr;

    Y_SAVELOAD_DEFINE_OVERRIDE(TableIndex_);
};

IRawParDoPtr CreateWriteNodeParDo(ssize_t tableIndex)
{
    return NYT::New<TWriteNodeParDo>(tableIndex);
}

////////////////////////////////////////////////////////////////////////////////

NYT::TTableSchema ToUnsortedSchema(const NYT::TTableSchema& schema)
{
    auto result = schema;
    for (auto& column : result.MutableColumns()) {
        column.ResetSortOrder();
    }
    result.UniqueKeys(false);
    return result;
}

NYT::TSortColumns GetSortColumns(const NYT::TTableSchema& schema)
{
    NYT::TSortColumns sortColumns;
    for (const auto& column : schema.Columns()) {
        if (column.SortOrder()) {
            sortColumns.Add(column.Name());
        }
    }
    return sortColumns;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
