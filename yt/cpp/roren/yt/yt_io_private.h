#pragma once

#include <yt/cpp/roren/interface/transforms.h>
#include <yt/cpp/roren/interface/private/serializable.h>
#include <yt/cpp/roren/interface/private/raw_data_flow.h>
#include <yt/cpp/roren/interface/private/raw_transform.h>

#include <yt/cpp/mapreduce/interface/common.h>
#include <yt/cpp/mapreduce/interface/serialize.h>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class IRawYtRead;
using IRawYtReadPtr = ::TIntrusivePtr<IRawYtRead>;

class IRawYtWrite;
using IRawYtWritePtr = ::TIntrusivePtr<IRawYtWrite>;

////////////////////////////////////////////////////////////////////////////////

class IYtNotSerializableJobInput
    : public IRawInput
{
public:
    virtual ssize_t GetInputIndex() {
        return 0;
    }
};

using IYtNotSerializableJobInputPtr = ::TIntrusivePtr<IYtNotSerializableJobInput>;

class IYtJobInput
    : public IYtNotSerializableJobInput
    , public ISerializable<IYtJobInput>
{ };

using IYtJobInputPtr = ::TIntrusivePtr<IYtJobInput>;

////////////////////////////////////////////////////////////////////////////////

class IYtJobOutput
    : public IRawOutput
    , public ISerializable<IYtJobOutput>
{
public:
    virtual int GetSinkCount() const = 0;
    virtual std::vector<int> GetSinkIndices() const = 0;
    virtual void SetSinkIndices(const std::vector<int>& sinkIndices) = 0;
};

using IYtJobOutputPtr = ::TIntrusivePtr<IYtJobOutput>;

////////////////////////////////////////////////////////////////////////////////

class TYtJobOutput
    : public virtual IYtJobOutput
{
public:
    explicit TYtJobOutput(int sinkIndex = -1);

    int GetSinkCount() const override;
    std::vector<int> GetSinkIndices() const override;
    void SetSinkIndices(const std::vector<int>& sinkIndices) override;

    void Save(IOutputStream* stream) const override;
    void Load(IInputStream* stream) override;

private:
    int SinkIndex_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

class IKvJobOutput
    : public virtual IYtJobOutput
{
public:
    virtual void AddKvToTable(const void* key, const void* value, ui64 tableIndex) = 0;
    using IYtJobOutput::AddRaw;
    virtual void AddRawToTable(const void* raw, ssize_t count, ui64 tableIndex) = 0;
};

using IKvJobOutputPtr = ::TIntrusivePtr<IKvJobOutput>;

////////////////////////////////////////////////////////////////////////////////

class IKvTableIndexJobOutput
    : public virtual IYtJobOutput
{
public:
    virtual void Add(const void* key, const void* value, ui64 tableIndex) = 0;
};

using IKvTableIndedxJobOutputPtr = ::TIntrusivePtr<IKvTableIndexJobOutput>;

////////////////////////////////////////////////////////////////////////////////

class IRawYtRead
    : public IRawRead
{
public:
    explicit IRawYtRead(NYT::TRichYPath path)
        : Path_(std::move(path))
    { }

    const NYT::TRichYPath& GetPath() const
    {
        return Path_;
    }

    [[nodiscard]] virtual IYtJobInputPtr CreateJobInput() const = 0;

    void Save(IOutputStream* stream) const override
    {
        SaveThroughYson(stream, Path_);
    }

    void Load(IInputStream* stream) override
    {
        LoadThroughYson(stream, Path_);
    }

private:
    NYT::TRichYPath Path_;
};

////////////////////////////////////////////////////////////////////////////////

class IRawYtWrite
    : public IRawWrite
{
public:
    IRawYtWrite(NYT::TRichYPath path, NYT::TTableSchema schema)
        : Path_(std::move(path))
        , Schema_(std::move(schema))
    { }

    const NYT::TRichYPath& GetPath() const
    {
        return Path_;
    }

    const NYT::TTableSchema& GetSchema() const
    {
        return Schema_;
    }

    virtual IYtJobOutputPtr CreateJobOutput(int sinkIndex = 0) const = 0;

    void Save(IOutputStream* stream) const override
    {
        SaveThroughYson(stream, Path_);
        SaveThroughYson(stream, Schema_);
    }

    void Load(IInputStream* stream) override
    {
        LoadThroughYson(stream, Path_);
        LoadThroughYson(stream, Schema_);
    }

    void AddRaw(const void*, ssize_t) override
    {
        Y_ABORT("not implemented");
    }

    void Close() override
    {
        Y_ABORT("not implemented");
    }

private:
    NYT::TRichYPath Path_;
    NYT::TTableSchema Schema_;
};

////////////////////////////////////////////////////////////////////////////////

IRawYtReadPtr MakeYtNodeInput(NYT::TRichYPath path);
IRawYtWritePtr MakeYtNodeWrite(NYT::TRichYPath path, NYT::TTableSchema tableSchema);

////////////////////////////////////////////////////////////////////////////////

IYtJobInputPtr CreateDecodingJobInput(TRowVtable rowVtables);
IYtJobInputPtr CreateDecodingJobInput(const std::vector<TRowVtable>& rowVtables);
IYtNotSerializableJobInputPtr CreateSplitKvJobNodeInput(
    const std::vector<TRowVtable>& rowVtables, NYT::TTableReaderPtr<NYT::TNode> tableReader);

IYtJobOutputPtr CreateEncodingJobOutput(const TRowVtable& rowVtable, int sinkIndex);

IKvJobOutputPtr CreateKvJobOutput(int sinkIndex, const std::vector<TRowVtable>& rowVtables);
IKvJobOutputPtr CreateKvJobOutput(int sinkIndex, IRawCoderPtr keyCoder, IRawCoderPtr valueCoder);

IYtJobOutputPtr CreateTeeJobOutput(std::vector<IYtJobOutputPtr> outputs);
IYtJobOutputPtr CreateParDoJobOutput(IRawParDoPtr rawParDo, std::vector<IYtJobOutputPtr> outputs);

////////////////////////////////////////////////////////////////////////////////

// Wrap output into ParDo.
// This ParDo returns void
IRawParDoPtr CreateOutputParDo(IYtJobOutputPtr output, TRowVtable rowVtable);

//
// Create ParDo that Encodes/Decodes roren rows to TNode.
//
// Create{Decoding,Encoding}**Value**NodeParDo works with TNode with single column "value"
// and roren row of arbitrary type. That value contains encoded representatino of row.
//
// CreateDecoding,Encoding**KeyValue**NodeParDo works with TNode with columns "key", "value"
// and roren rows that are TKV<?,?>. Key part of TKV goes into "key" column and value part into "value".
IRawParDoPtr CreateDecodingValueNodeParDo(TRowVtable rowVtable);
IRawParDoPtr CreateEncodingValueNodeParDo(TRowVtable rowVtable);

IRawParDoPtr CreateDecodingKeyValueNodeParDo(TRowVtable rowVtable);
IRawParDoPtr CreateEncodingKeyValueNodeParDo(TRowVtable rowVtable);

//
// Create ParDo that reads TNode rows from stdin.
//
// ParDo expects "impulse" as input (i.e. single integer of value '0')
// When it receives it it starts reading data and passes it down the pipeline.
IRawParDoPtr CreateReadNodeImpulseParDo(ssize_t tableCount);
IRawParDoPtr CreateWriteNodeParDo(ssize_t tableIndex);

////////////////////////////////////////////////////////////////////////////////

inline int GetOutputFD(size_t output)
{
    return output * 3 + 1;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren::NPrivate
