#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/table_client.h>
#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//! Attaches one synthetic offshore chunk for each source URI.
TFuture<void> AttachTable(
    const NYPath::TRichYPath& richPath,
    std::vector<std::string> sourceUris,
    const NApi::TAttachTableOptions& options,
    NApi::NNative::IClientPtr client,
    NApi::ITransactionPtr transaction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
