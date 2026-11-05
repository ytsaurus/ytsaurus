#pragma once

#include <yt/yt/tests/cpp/test_base/api_test_base.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/client/table_client/public.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/permission.h>
#include <yt/yt/core/ytree/public.h>

namespace NYT::NCppTests {

////////////////////////////////////////////////////////////////////////////////

class TQueueTestBase
    : public TDynamicTablesTestBase
{
public:
    static constexpr auto RegistrationTablePath = "//sys/queue_agents/consumer_registrations";

    static void SetUpTestCase();

    class TDynamicTable final
    {
    public:
        TDynamicTable(
            NYPath::TRichYPath path,
            NTableClient::TTableSchemaPtr schema,
            const NYTree::IAttributeDictionaryPtr& extraAttributes = NYTree::CreateEphemeralAttributes());

        ~TDynamicTable();

        const NTableClient::TTableSchemaPtr& GetSchema() const;

        const NYPath::TYPath& GetPath() const;

        const NYPath::TRichYPath& GetRichPath() const;

        NYPath::TRichYPath GetRichPathWithCluster() const;

    private:
        NYPath::TRichYPath RichPath_;
        NYPath::TYPath Path_;
        NTableClient::TTableSchemaPtr Schema_;
    };

    using TDynamicTablePtr = TIntrusivePtr<TDynamicTable>;

    static void WriteSharedRange(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const TSharedRange<NTableClient::TUnversionedRow>& range);

    static void WriteSingleRow(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        NTableClient::TUnversionedRow row);

    static void WriteSingleRow(
        const NYPath::TYPath& path,
        const NTableClient::TNameTablePtr& nameTable,
        const std::vector<std::string>& values);

    static void WaitForRowCount(const NYPath::TYPath& path, i64 rowCount);

    std::tuple<TDynamicTablePtr, TDynamicTablePtr, NTableClient::TNameTablePtr> CreateQueueAndConsumer(
        const std::string& testName,
        std::optional<bool> useNativeTabletNodeApi = {},
        int queueTabletCount = 1) const;

    void CreateQueueProducer(const NYPath::TRichYPath& path);

    // NB: Only creates user once per test YT instance.
    NApi::IClientPtr CreateUser(const std::string& name) const;

    void AssertPermission(
        const std::string& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission,
        NSecurityClient::ESecurityAction action) const;

    void AssertPermissionAllowed(
        const std::string& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission) const;

    void AssertPermissionDenied(
        const std::string& user,
        const NYPath::TYPath& path,
        NYTree::EPermission permission) const;

    static void CreateTableOnce(const NYPath::TYPath& path, const NTableClient::TTableSchemaPtr& schema);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCppTests
