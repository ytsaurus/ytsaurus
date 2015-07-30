#pragma once

#include <mapreduce/yt/interface/io.h>

namespace NYT {

class TProxyInput;

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
    virtual size_t GetTableIndex() const override;
    virtual void NextKey() override;

private:
    THolder<TProxyInput> Input_;
    bool Valid_;
    bool Finished_;
    size_t TableIndex_;
    TYaMRRow Row_;

private:
    void CheckValidity() const;

    size_t Load(void* buf, size_t len);

    bool ReadInteger(i32* result, bool acceptEndOfStream = false);
    void ReadField(Stroka* result, i32 length);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
