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
    explicit TProtoTableWriter(THolder<TProxyOutput> output);
    ~TProtoTableWriter() override;

    void AddRow(const Message& row, size_t tableIndex) override;

    size_t GetStreamCount() const override;
    TOutputStream* GetStream(size_t tableIndex) const override;

private:
    THolder<TNodeTableWriter> NodeWriter_;
};

////////////////////////////////////////////////////////////////////////////////

class TLenvalProtoTableWriter
    : public IProtoWriterImpl
{
public:
    explicit TLenvalProtoTableWriter(THolder<TProxyOutput> output);
    ~TLenvalProtoTableWriter() override;

    void AddRow(const Message& row, size_t tableIndex) override;

    size_t GetStreamCount() const override;
    TOutputStream* GetStream(size_t tableIndex) const override;

private:
    THolder<TProxyOutput> Output_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
