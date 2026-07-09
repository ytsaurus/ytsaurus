#pragma once

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ypath/public.h>

#include <optional>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Thin helper over a pipeline's flow_control dynamic table — a simple
//! "key (string) -> value (any/YSON)" store. Keeps callers free of the
//! name-table / row-buffer plumbing of the dynamic-table API.
struct TControlTable
{
    //! Looks up the YSON value stored under `key`, or std::nullopt if the row is absent.
    static TFuture<std::optional<NYson::TYsonString>> Read(
        const NApi::IClientPtr& client,
        const NYPath::TYPath& controlTablePath,
        TStringBuf key);

    //! Appends a write of `key -> value` to `transaction`; the caller commits.
    static void Write(
        const NApi::ITransactionPtr& transaction,
        const NYPath::TYPath& controlTablePath,
        TStringBuf key,
        const NYson::TYsonString& value);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
