#pragma once

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/public.h>
#include <yt/yt/client/api/client.h>
#include <yt/yt/client/ypath/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TFuture<NApi::TAttachTableResult> AttachTable(
    NYPath::TRichYPath richPath,
    NTableClient::TExternalSourceSpec sourceSpec,
    NApi::TAttachTableOptions options,
    NApi::NNative::IClientPtr client,
    NApi::ITransactionPtr transaction);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
