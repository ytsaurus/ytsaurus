#pragma once

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/ypath/public.h>
#include <yt/yt/core/actions/future.h>
#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NTableClient {

TFuture<void> AttachTable(
    const NYPath::TRichYPath& richPath,
    NApi::NNative::IClientPtr client,
    NApi::ITransactionPtr transaction,
    std::vector<std::string> sources);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
