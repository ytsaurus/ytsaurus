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

    size_t GetStreamCount() const override;
    IOutputStream* GetStream(size_t tableIndex) const override;
    void Abort() override;

private:
    THolder<TProxyOutput> Output_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
