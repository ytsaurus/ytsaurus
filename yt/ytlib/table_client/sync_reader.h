#pragma once

#include "value.h"
#include <ytlib/misc/ref_counted.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISyncReader 
    : public virtual TRefCounted
{
    virtual void Open() = 0;
    virtual void NextRow() = 0;
    virtual bool IsValid() const = 0;
    virtual const TRow& GetRow() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IAsyncReader;

class TSyncReader 
    : public ISyncReader
{
public:
    TSyncReader(IAsyncReader* asyncReader);

    void Open();
    void NextRow();
    bool IsValid() const;
    const TRow& GetRow() const;

private:
    TIntrusivePtr<IAsyncReader> AsyncReader;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
