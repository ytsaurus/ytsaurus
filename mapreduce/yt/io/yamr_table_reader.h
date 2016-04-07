#pragma once

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
{
public:
    explicit TYaMRTableReader(THolder<TProxyInput> input);
    ~TYaMRTableReader() override;

    const TYaMRRow& GetRow() const override;
    bool IsValid() const override;
    void Next() override;
    ui32 GetTableIndex() const override;
    ui64 GetRowIndex() const override;
    void NextKey() override;

private:
    THolder<TProxyInput> Input_;
    bool Valid_ = true;
    bool Finished_ = false;
    ui32 TableIndex_ = 0;
    TMaybe<ui64> RowIndex_;
    TMaybe<ui32> RangeIndex_;
    bool AtStart_ = true;

    TYaMRRow Row_;
    Stroka Key_;
    Stroka SubKey_;
    Stroka Value_;

private:
    void CheckValidity() const;

    size_t Load(void* buf, size_t len);

    template <class T>
    bool ReadInteger(T* result, bool acceptEndOfStream = false);
    void ReadField(Stroka* result, i32 length);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
