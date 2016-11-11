#pragma once

#include <mapreduce/yt/interface/io.h>

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
        yvector<const ::google::protobuf::Descriptor*>&& descriptors);
    ~TProtoTableWriter() override;

    void AddRow(const Message& row, size_t tableIndex) override;

    size_t GetStreamCount() const override;
    TOutputStream* GetStream(size_t tableIndex) const override;

private:
    THolder<TNodeTableWriter> NodeWriter_;
    yvector<const ::google::protobuf::Descriptor*> Descriptors_;
};

////////////////////////////////////////////////////////////////////////////////

class TLenvalProtoTableWriter
    : public IProtoWriterImpl
{
public:
    TLenvalProtoTableWriter(
        THolder<TProxyOutput> output,
        yvector<const ::google::protobuf::Descriptor*>&& descriptors);
    ~TLenvalProtoTableWriter() override;

    void AddRow(const Message& row, size_t tableIndex) override;

    size_t GetStreamCount() const override;
    TOutputStream* GetStream(size_t tableIndex) const override;

private:
    THolder<TProxyOutput> Output_;
    yvector<const ::google::protobuf::Descriptor*> Descriptors_;
};

// Sometime useful outside mapreduce/yt
TNode MakeNodeFromMessage(const ::google::protobuf::Message& row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
