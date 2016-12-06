#pragma once

#include "public.h"

#include <yt/core/misc/error.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! A pipe connecting a schemaful writer to a schemaful reader.
class TSchemafulPipe
    : public TIntrinsicRefCounted
{
public:
    TSchemafulPipe();
    ~TSchemafulPipe();

    //! Returns the reader side of the pipe.
    ISchemafulReaderPtr GetReader() const;

    //! Returns the writer side of the pipe.
    ISchemafulWriterPtr GetWriter() const;

    //! When called, propagates the error to the reader.
    void Fail(const TError& error);

private:
    class TImpl;
    typedef TIntrusivePtr<TImpl> TImplPtr;

    struct TData;
    typedef TIntrusivePtr<TData> TDataPtr;

    class TReader;
    typedef TIntrusivePtr<TReader> TReaderPtr;

    class TWriter;
    typedef TIntrusivePtr<TWriter> TWriterPtr;


    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TSchemafulPipe)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
