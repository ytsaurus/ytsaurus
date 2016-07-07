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
    ~TYaMRTableWriter() override;

    void AddRow(const TYaMRRow& row, size_t tableIndex) override;
    void Finish() override;

private:
    THolder<TProxyOutput> Output_;
    yvector<TMutex> Locks_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
