#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NTableClient {

TFuture<void> AttachTable(
    const NYPath::TRichYPath& richPath,
    const NApi::TAttachTableOptions& options,
    NApi::NNative::IClientPtr client,
    NApi::ITransactionPtr transaction,
    std::vector<std::string> sources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
