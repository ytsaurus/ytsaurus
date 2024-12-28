#pragma once

#include <yt/yt/ytlib/table_client/blob_table_writer.h>

namespace NYT::NServer {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TBlobTableSchema GetStderrBlobTableSchema();

NTableClient::TBlobTableSchema GetCoreBlobTableSchema();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServer
