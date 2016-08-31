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

    size_t GetStreamCount() const override;
    TOutputStream* GetStream(size_t tableIndex) const override;

private:
    THolder<TProxyOutput> Output_;
    yvector<THolder<TYsonWriter>> Writers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
