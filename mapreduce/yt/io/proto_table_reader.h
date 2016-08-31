#pragma once

#include "lenval_table_reader.h"

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

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;

private:
    THolder<TNodeTableReader> NodeReader_;
};

////////////////////////////////////////////////////////////////////////////////

class TLenvalProtoTableReader
    : public IProtoReaderImpl
    , public TLenvalTableReader
{
public:
    explicit TLenvalProtoTableReader(THolder<TProxyInput> input);
    ~TLenvalProtoTableReader() override;

    void ReadRow(Message* row) override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;

protected:
    void OnRowStart() override;

private:
    bool RowTaken_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
