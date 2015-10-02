#pragma once

#include <mapreduce/yt/interface/io.h>

namespace NYT {

class TProxyInput;
class TAuth;

////////////////////////////////////////////////////////////////////////////////

TMaybe<TNode> GetTableFormat(const TAuth& auth, const TRichYPath& path);
TMaybe<TNode> GetTableFormats(const TAuth& auth, const yvector<TRichYPath>& paths);

////////////////////////////////////////////////////////////////////////////////

class TYaMRTableReader
    : public IYaMRReaderImpl
{
public:
    explicit TYaMRTableReader(THolder<TProxyInput> input);
    ~TYaMRTableReader();

    virtual const TYaMRRow& GetRow() const override;
    virtual bool IsValid() const override;
    virtual void Next() override;
    virtual ui32 GetTableIndex() const override;
    virtual ui64 GetRowIndex() const override;
    virtual void NextKey() override;

private:
    THolder<TProxyInput> Input_;
    bool Valid_ = true;
    bool Finished_ = false;
    ui32 TableIndex_ = 0;
    TMaybe<ui64> RowIndex_;
    TYaMRRow Row_;

private:
    void CheckValidity() const;

    size_t Load(void* buf, size_t len);

    template <class T>
    bool ReadInteger(T* result, bool acceptEndOfStream = false);
    void ReadField(Stroka* result, i32 length);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
