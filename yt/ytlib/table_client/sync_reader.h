#pragma once

#include "public.h"
#include <ytlib/ytree/public.h>
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
    virtual const NYTree::TYson& GetRowAttributes() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSyncReaderAdapter 
    : public ISyncReader
{
public:
    TSyncReaderAdapter(IAsyncReaderPtr asyncReader);

    void Open();
    void NextRow();
    bool IsValid() const;

    const TRow& GetRow() const;
    const NYTree::TYson& GetRowAttributes() const;

private:
    IAsyncReaderPtr AsyncReader;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
