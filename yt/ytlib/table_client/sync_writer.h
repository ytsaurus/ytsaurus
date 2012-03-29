#pragma once

#include "value.h"
#include <ytlib/misc/ref_counted.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISyncWriter
    : public virtual TRefCounted
{
    virtual void Open() = 0;
    virtual void Write(const TColumn& column, TValue value) = 0;
    virtual void EndRow() = 0;
    virtual void Close() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TValidatingWriter;

class TSyncWriter
    : public ISyncWriter
{
public:
    typedef TIntrusivePtr<TSyncWriter> TPtr;

    TSyncWriter(TValidatingWriter* writer);

    void Open();
    void Write(const TColumn& column, TValue value);
    void EndRow();
    void Close();

private:
    TAutoPtr<TValidatingWriter> ValidatingWriter;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
