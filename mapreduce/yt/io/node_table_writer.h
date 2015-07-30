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
    ~TNodeTableWriter();

    virtual void AddRow(const TNode& row, size_t tableIndex) override;
    virtual void Finish() override;

private:
    THolder<TProxyOutput> Output_;
    yvector<TSimpleSharedPtr<TYsonWriter>> Writers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
