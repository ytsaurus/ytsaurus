#pragma once

#include "value.h"
#include <ytlib/misc/ref_counted.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISyncTableReader 
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<ISyncTableReader> TPtr;

    virtual void Open() = 0;
    virtual void NextRow() = 0;
    virtual bool IsValid() const = 0;
    virtual const TRow& GetRow() const = 0;
    virtual const TKey& GetKey() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IAsyncReader;

class TSyncReaderAdapter 
    : public ISyncTableReader
{
public:
    typedef TIntrusivePtr<TSyncReaderAdapter> TPtr;

    TSyncReaderAdapter(IAsyncReader* asyncReader);

    void Open();
    void NextRow();
    bool IsValid() const;
    const TRow& GetRow() const;
    const TKey& GetKey() const;

private:
    TIntrusivePtr<IAsyncReader> AsyncReader;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
