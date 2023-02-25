#pragma once

#include <yt/cpp/mapreduce/interface/io.h>

namespace NYT {

class TProxyOutput;
class TNodeTableWriter;

////////////////////////////////////////////////////////////////////////////////

class TProtoTableWriter
    : public IProtoWriterImpl
{
public:
    TProtoTableWriter(
        THolder<TProxyOutput> output,
        TVector<const ::google::protobuf::Descriptor*>&& descriptors);
    ~TProtoTableWriter() override;

    void AddRow(const Message& row, size_t tableIndex) override;
    void AddRow(Message&& row, size_t tableIndex) override;

    size_t GetTableCount() const override;
    void FinishTable(size_t) override;
    void Abort() override;

private:
    THolder<TNodeTableWriter> NodeWriter_;
    TVector<const ::google::protobuf::Descriptor*> Descriptors_;
};

////////////////////////////////////////////////////////////////////////////////

class TLenvalProtoTableWriter
    : public IProtoWriterImpl
{
public:
    TLenvalProtoTableWriter(
        THolder<TProxyOutput> output,
        TVector<const ::google::protobuf::Descriptor*>&& descriptors);
    ~TLenvalProtoTableWriter() override;

    void AddRow(const Message& row, size_t tableIndex) override;
    void AddRow(Message&& row, size_t tableIndex) override;

    size_t GetTableCount() const override;
    void FinishTable(size_t) override;
    void Abort() override;

private:
    THolder<TProxyOutput> Output_;
    TVector<const ::google::protobuf::Descriptor*> Descriptors_;
};

// Sometime useful outside mapreduce/yt
TNode MakeNodeFromMessage(const ::google::protobuf::Message& row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
