#include "client_impl.h"
#include "journal_reader.h"
#include "journal_writer.h"

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/journal_client/journal_ypath_proxy.h>

namespace NYT::NApi::NNative {

using namespace NYPath;
using namespace NObjectClient;
using namespace NJournalClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

IJournalReaderPtr TClient::CreateJournalReader(
    const TYPath& path,
    const TJournalReaderOptions& options)
{
    return NNative::CreateJournalReader(this, path, options);
}

IJournalWriterPtr TClient::CreateJournalWriter(
    const TYPath& path,
    const TJournalWriterOptions& options)
{
    return NNative::CreateJournalWriter(this, path, options);
}

void TClient::DoTruncateJournal(
    const NYPath::TYPath& path,
    i64 rowCount,
    const TTruncateJournalOptions& options)
{
    auto proxy = CreateWriteProxy<TObjectServiceProxy>();

    auto req = TJournalYPathProxy::Truncate(path);
    req->set_path(path);
    req->set_row_count(rowCount);

    SetPrerequisites(req, options);
    SetMutationId(req, options);

    WaitFor(proxy->Execute(req))
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
