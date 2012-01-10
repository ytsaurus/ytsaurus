#pragma once

#include "common.h"
#include "value.h"
#include "schema.h"

#include <ytlib/misc/ref_counted_base.h>
#include <ytlib/misc/async_stream_state.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Before using reader client must call #AsyncOpen and ensure that returned 
//! future is set with OK error code.
//! Method #AsyncNextRow switches reader to next row (and should be called 
//! before the first row), and no other client calls are allowed until 
//! returned future is set. Before calling #AsyncNextRow client should check
//! that next row exists using #HasNextRow.
//! When row is fetched client can iterate on columns using #NextColumn and
//! get table data with #GetColumn and #GetValue.
struct IAsyncReader
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IAsyncReader> TPtr;

    virtual TAsyncError::TPtr AsyncOpen() = 0;

    virtual bool HasNextRow() const = 0;
    virtual TAsyncError::TPtr AsyncNextRow() = 0;

    virtual bool NextColumn() = 0;
    virtual TValue GetValue() const = 0;
    virtual TColumn GetColumn() const = 0;

    virtual void Cancel(const TError& error) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT