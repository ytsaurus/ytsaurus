#pragma once

#include "public.h"
#include "file_reader_base.h"

#include <ytlib/file_client/file_reader_base.h>
#include <ytlib/object_client/object_service_proxy.h>

namespace NYT {
namespace NFileClient {

////////////////////////////////////////////////////////////////////////////////

//! A client-side facade for reading files.
/*!
 *  The client must call #Open and then read the file block-by-block
 *  calling #Read.
 */
class TFileReader
    : public TFileReaderBase
{
public:
    //! Initializes an instance.
    TFileReader(
        TFileReaderConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NTransactionClient::ITransactionPtr transaction,
        NChunkClient::IBlockCachePtr blockCache,
        const NYPath::TYPath& path);

    //! Opens the reader.
    void Open();

    //! Returns the size of the file.
    i64 GetSize() const;

    //! Returns the file name (as provided by the master).
    Stroka GetFileName() const;

    //! Returns the executable flag.
    bool IsExecutable();

private:
    NTransactionClient::ITransactionPtr Transaction;
    NYPath::TYPath Path;

    DECLARE_THREAD_AFFINITY_SLOT(Client);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileClient
} // namespace NYT
