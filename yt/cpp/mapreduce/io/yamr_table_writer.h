#pragma once

#include <yt/cpp/mapreduce/interface/io.h>

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
    void AddRow(TYaMRRow&& row, size_t tableIndex) override;

    size_t GetTableCount() const override;
    void FinishTable(size_t) override;
    void Abort() override;

private:
    THolder<TProxyOutput> Output_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
