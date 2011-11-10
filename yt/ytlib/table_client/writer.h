#pragma once

#include "common.h"
#include "value.h"
#include "schema.h"

#include "../misc/ref_counted_base.h"
#include "../misc/async_stream_state.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IWriter
    : public virtual TRefCountedBase
{
    virtual TAsyncStreamState::TAsyncResult::TPtr AsyncInit() = 0;

    virtual void Write(const TColumn& column, TValue value) = 0;

    virtual TAsyncStreamState::TAsyncResult::TPtr AsyncEndRow() = 0;
    virtual TAsyncStreamState::TAsyncResult::TPtr AsyncClose() = 0;

    virtual void Cancel(const Stroka& errorMessage) = 0;

    // Sync calls
    void EndRow();
    void Close();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT