#pragma once

#include "public.h"

#include <core/misc/error.h>

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

//! A pipe connecting a schemed writer to a schemed reader.
class TSchemedPipe
    : public TIntrinsicRefCounted
{
public:
    TSchemedPipe();
    ~TSchemedPipe();

    //! Returns the reader side of the pipe.
    ISchemedReaderPtr GetReader() const;

    //! Returns the writer side of the pipe.
    ISchemedWriterPtr GetWriter() const;

    //! When called, propagates the error to the reader.
    void Fail(const TError& error);

private:
    class TImpl;
    typedef TIntrusivePtr<TImpl> TImplPtr;
    
    class TData;
    typedef TIntrusivePtr<TData> TDataPtr;

    class TReader;
    typedef TIntrusivePtr<TReader> TReaderPtr;

    class TWriter;
    typedef TIntrusivePtr<TWriter> TWriterPtr;


    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TSchemedPipe)

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
