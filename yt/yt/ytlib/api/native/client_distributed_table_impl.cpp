#include "client_impl.h"

#include "config.h"

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NFlow;
using namespace NFlow::NController;

////////////////////////////////////////////////////////////////////////////////

TFuture<TDistributedWriteSessionPtr> TClient::StartDistributedWriteSession(
    const NYPath::TRichYPath& path,
    const TDistributedWriteSessionStartOptions& options)
{
    // Not Implemented.
    Y_UNUSED(path, options);
    YT_ABORT();
}

TFuture<void> TClient::FinishDistributedWriteSession(
    TDistributedWriteSessionPtr session,
    const TDistributedWriteSessionFinishOptions& options)
{
    // Not Implemented.
    Y_UNUSED(session, options);
    YT_ABORT();
}

TFuture<ITableWriterPtr> TClient::CreateParticipantTableWriter(
    const TDistributedWriteCookiePtr& cookie,
    const TParticipantTableWriterOptions& options)
{
    // Not Implemented.
    Y_UNUSED(cookie, options);
    YT_ABORT();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
