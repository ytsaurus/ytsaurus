#pragma once

#include <mapreduce/yt/interface/io.h>
#include <library/yson/public.h>

namespace NYT {

class TProxyOutput;
class TYsonWriter;

////////////////////////////////////////////////////////////////////////////////

class TNodeTableWriter
    : public INodeWriterImpl
{
public:
    explicit TNodeTableWriter(THolder<TProxyOutput> output, EYsonFormat format = YF_BINARY);
    ~TNodeTableWriter() override;

    void AddRow(const TNode& row, size_t tableIndex) override;

    size_t GetStreamCount() const override;
    IOutputStream* GetStream(size_t tableIndex) const override;
    void Abort() override;

private:
    THolder<TProxyOutput> Output_;
    TVector<THolder<TYsonWriter>> Writers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
