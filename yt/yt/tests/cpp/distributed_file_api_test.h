#pragma once

#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>
#include <yt/yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/yt/ytlib/object_client/config.h>
#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/file_client/file_ypath_proxy.h>

#include <yt/yt/client/api/distributed_table_session.h>
#include <yt/yt/client/api/internal_client.h>
#include <yt/yt/client/api/table_reader.h>
#include <yt/yt/client/api/table_writer.h>

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

class TDistributedFileApiTest
    : public TApiTestBase
{
public:
    void SetUp() override;

public:
    static void CreateFile(const TString& path);

    static NApi::TDistributedWriteFileSessionWithCookies StartDistributedWriteSession(
        int cookieCount,
        NCypressClient::TTransactionId txId = {},
        std::optional<TDuration> timeout = std::nullopt);

    static void PingDistributedWriteSession(const NApi::TSignedDistributedWriteFileSessionPtr& session);

    static void FinishDistributedWriteSession(
        NApi::TSignedDistributedWriteFileSessionPtr session,
        std::vector<NApi::TSignedWriteFileFragmentResultPtr> results = {});

    static NApi::TSignedWriteFileFragmentResultPtr DistributedWriteFile(
        const NApi::TSignedWriteFileFragmentCookiePtr& cookie,
        const TString& data);

    static TString ReadFile();

private:
    static inline TString File_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCppTests
