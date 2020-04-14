#pragma once

#include <yt/ytlib/api/connection.h>

#include <yt/core/misc/shared_range.h>

#include <yt/core/rpc/public.h>

#include <yt/core/test_framework/framework.h>

namespace NYT::NCppTests {

////////////////////////////////////////////////////////////////////////////////

class TApiTestBase
    : public ::testing::Test
{
protected:
    static NApi::IConnectionPtr Connection_;
    static NApi::IClientPtr Client_;

    static std::vector<NApi::IConnectionPtr> RemoteConnections_;
    static std::vector<NApi::IClientPtr> RemoteClients_;

    static void SetUpTestCase();
    static void TearDownTestCase();

    static NApi::IClientPtr CreateClient(const TString& userName);
    static NApi::IClientPtr CreateRemoteClient(int remoteClusterIndex, const TString& userName);
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicTablesTestBase
    : public TApiTestBase
{
public:
    static void TearDownTestCase();

protected:
    static NYPath::TYPath Table_;

    static void SetUpTestCase();

    static void CreateTable(
        const TString& tablePath,
        const TString& schema);

    static void SyncMountTable(const NYPath::TYPath& path);
    static void SyncUnmountTable(const NYPath::TYPath& path);

    static void WaitUntilEqual(
        const NYPath::TYPath& path,
        const TString& expected);
    static void WaitUntil(
        std::function<bool()> predicate,
        const TString& errorMessage);

    static std::tuple<TSharedRange<NTableClient::TUnversionedRow>, NTableClient::TNameTablePtr> PrepareUnversionedRow(
        const std::vector<TString>& names,
        const TString& rowString);
    static void WriteUnversionedRow(
        std::vector<TString> names,
        const TString& rowString,
        const NApi::IClientPtr& client = Client_);
    static void WriteUnversionedRows(
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TUnversionedRow> rows,
        const NApi::IClientPtr& client = Client_);

    static std::tuple<TSharedRange<NTableClient::TVersionedRow>, NTableClient::TNameTablePtr> PrepareVersionedRow(
        const std::vector<TString>& names,
        const TString& keyYson,
        const TString& valueYson);
    static void WriteVersionedRow(
        std::vector<TString> names,
        const TString& keyYson,
        const TString& valueYson,
        const NApi::IClientPtr& client = Client_);
    static void WriteVersionedRows(
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TVersionedRow> rows,
        const NApi::IClientPtr& client = Client_);

private:
    static void RemoveUserObjects(const NYPath::TYPath& path);
    static void RemoveTabletCells(
        std::function<bool(const TString&)> filter = [] (const TString&) { return true; });
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCppTests
