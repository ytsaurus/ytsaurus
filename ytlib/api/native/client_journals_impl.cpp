#include "client_impl.h"
#include "journal_reader.h"
#include "journal_writer.h"

namespace NYT::NApi::NNative {

using namespace NYPath;

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
