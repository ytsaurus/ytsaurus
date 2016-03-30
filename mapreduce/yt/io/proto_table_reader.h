#pragma once

#include <mapreduce/yt/interface/io.h>

namespace NYT {

class TProxyInput;
class TNodeTableReader;

////////////////////////////////////////////////////////////////////////////////

class TProtoTableReader
    : public IProtoReaderImpl
{
public:
    explicit TProtoTableReader(THolder<TProxyInput> input);
    ~TProtoTableReader() override;

    void ReadRow(Message* row) override;
    void SkipRow() override;
    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;

private:
    THolder<TNodeTableReader> NodeReader_; // proto over yson
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
