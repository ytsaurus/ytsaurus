#pragma once

#include <mapreduce/yt/interface/io.h>

namespace NYT {

class TProxyOutput;

////////////////////////////////////////////////////////////////////////////////

class TYaMRTableWriter
    : public IYaMRWriterImpl
{
public:
    explicit TYaMRTableWriter(THolder<TProxyOutput> output);
    ~TYaMRTableWriter();

    virtual void AddRow(const TYaMRRow& row, size_t tableIndex) override;
    virtual void Finish() override;

private:
    THolder<TProxyOutput> Output_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
