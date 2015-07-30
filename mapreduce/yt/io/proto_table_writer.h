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
    ~TProtoTableWriter();

    virtual void AddRow(const Message& row, size_t tableIndex) override;
    virtual void Finish() override;

private:
    THolder<TNodeTableWriter> NodeWriter_; // proto over yson
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
