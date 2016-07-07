#pragma once

#include <mapreduce/yt/interface/io.h>

namespace NYT {

class TProxyOutput;
class TYsonWriter;

////////////////////////////////////////////////////////////////////////////////

class TNodeTableWriter
    : public INodeWriterImpl
{
public:
    explicit TNodeTableWriter(THolder<TProxyOutput> output);
    ~TNodeTableWriter() override;

    void AddRow(const TNode& row, size_t tableIndex) override;
    void Finish() override;

private:
    THolder<TProxyOutput> Output_;
    yvector<THolder<TYsonWriter>> Writers_;
    yvector<TMutex> Locks_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
