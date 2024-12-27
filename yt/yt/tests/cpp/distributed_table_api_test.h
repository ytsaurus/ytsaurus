#pragma once

#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/config.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/table_client/table_ypath_proxy.h>

#include <yt/yt/client/api/distributed_table_session.h>
#include <yt/yt/client/api/internal_client.h>
#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/table_writer.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/transaction_client/helpers.h>
#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/log_manager.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/yson/string.h>

#include <util/datetime/base.h>

#include <util/random/random.h>

namespace NYT::NCppTests {

////////////////////////////////////////////////////////////////////////////////

class TDistributedTableApiTest
    : public TApiTestBase
{
public:
    static void CreateStaticTable(
        const TString& tablePath,
        const TString& schema);

    static void WriteTable(
        std::vector<TString> columnNames,
        std::vector<TString> rowStrings,
        bool append);

    static std::vector<TString> ReadTable();

    static NApi::TDistributedWriteSessionWithCookies StartDistributedWriteSession(
        bool append,
        int cookieCount,
        std::optional<NCypressClient::TTransactionId> txId = {});

    // Append is decided upon session opening.
    static NApi::TSignedWriteFragmentResultPtr DistributedWriteTable(
        const NApi::TSignedWriteFragmentCookiePtr& cookie,
        std::vector<TString> columnNames,
        std::vector<TString> rowStrings);

    static void PingDistributedWriteSession(
        const NApi::TSignedDistributedWriteSessionPtr& session);

    static void FinishDistributedWriteSession(
        NApi::TSignedDistributedWriteSessionPtr session,
        std::vector<NApi::TSignedWriteFragmentResultPtr> results = {},
        int chunkListsPerAttachRequest = 42);

private:
    static inline TString Table_;

    static NYPath::TRichYPath MakeRichPath(bool append);

    static TSharedRange<NTableClient::TUnversionedRow> YsonRowsToUnversionedRows(
        std::vector<TString> rowStrings);

    static void DoWriteTable(
        std::vector<TString> columnNames,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        bool append);

    static std::vector<TString> DoReadTable();

    static NApi::TSignedWriteFragmentResultPtr DoDistributedWriteTable(
        const NApi::TSignedWriteFragmentCookiePtr& cookie,
        std::vector<TString> columnNames,
        TSharedRange<NTableClient::TUnversionedRow> rows);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCppTests
