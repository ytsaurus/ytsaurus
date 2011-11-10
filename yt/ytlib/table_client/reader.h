#pragma once

#include "common.h"
#include "value.h"
#include "schema.h"

#include "../misc/ref_counted_base.h"
#include "../misc/async_stream_state.h"

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct IReader
    : public virtual TRefCountedBase
{
    virtual bool HasRow() = 0;
    virtual TAsyncStreamState::TAsyncResult::TPtr AsyncNextRow() = 0;

    virtual bool NextColumn() = 0;
    virtual TValue GetValue() = 0;
    virtual TColumn GetColumn() = 0;

    virtual void Cancel(const Stroka& errorMessage) = 0;

    //! Sync call
    void NextRow();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT