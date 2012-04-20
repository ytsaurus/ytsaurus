#pragma once

#include "public.h"

#include <ytlib/ytree/public.h>
#include <ytlib/misc/ref_counted.h>
#include <ytlib/misc/error.h>

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
    : public virtual TRefCounted
{
    virtual TAsyncError AsyncOpen() = 0;

    virtual TAsyncError AsyncNextRow() = 0;
    virtual bool IsValid() const = 0;

    virtual const TRow& GetRow() const = 0;
    virtual const TKey& GetKey() const = 0;
    virtual const NYTree::TYson& GetRowAttributes() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT