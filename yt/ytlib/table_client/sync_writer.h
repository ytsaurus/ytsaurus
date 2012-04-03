#pragma once

#include "value.h"
#include <ytlib/misc/ref_counted.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct ISyncTableWriter
    : public virtual TRefCounted
{
    typedef TIntrusivePtr<ISyncTableWriter> TPtr;

    virtual void Open() = 0;
    virtual void Write(const TColumn& column, TValue value) = 0;
    virtual void EndRow() = 0;
    virtual void Close() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TValidatingWriter;

class TSyncValidatingAdaptor
    : public ISyncTableWriter
{
public:
    typedef TIntrusivePtr<TSyncValidatingAdaptor> TPtr;

    TSyncValidatingAdaptor(TValidatingWriter* writer);

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
