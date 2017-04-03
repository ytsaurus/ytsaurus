#pragma once

#include "lenval_table_reader.h"

#include <mapreduce/yt/interface/io.h>

namespace NYT {

class TProxyInput;
struct TAuth;

////////////////////////////////////////////////////////////////////////////////

TMaybe<TNode> GetTableFormat(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TRichYPath& path);

TMaybe<TNode> GetTableFormats(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const yvector<TRichYPath>& paths);

////////////////////////////////////////////////////////////////////////////////

class TYaMRTableReader
    : public IYaMRReaderImpl
    , public TLenvalTableReader
{
public:
    explicit TYaMRTableReader(::TIntrusivePtr<TProxyInput> input);
    ~TYaMRTableReader() override;

    const TYaMRRow& GetRow() const override;

    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;

private:
    void ReadField(Stroka* result, i32 length);

    void ReadRow();
    void SkipRow() override;

    TYaMRRow Row_;
    Stroka Key_;
    Stroka SubKey_;
    Stroka Value_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
