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
    void Finish() override;

private:
    THolder<TNodeTableWriter> NodeWriter_; // proto over yson
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
