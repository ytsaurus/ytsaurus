#pragma once

#include <yt/yt/ytlib/api/connection.h>

#include <yt/yt/core/misc/shared_range.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT {
namespace NCppTests {

////////////////////////////////////////////////////////////////////////////////

class TApiTestBase
    : public ::testing::Test
{
protected:
    static NApi::IConnectionPtr Connection_;
    static NApi::IClientPtr Client_;
    static TString ClusterName_;

    static void SetUpTestCase();

    static void TearDownTestCase();

    static NApi::IClientPtr CreateClient(const TString& userName);

    static void WaitUntilEqual(const NYPath::TYPath& path, const TString& expected);

    static void WaitUntil(
        std::function<bool()> predicate,
        const TString& errorMessage);
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
        const TString& schema,
        bool mount = true);

    static void SyncMountTable(const NYPath::TYPath& path);
    static void SyncFreezeTable(const NYPath::TYPath& path);
    static void SyncUnfreezeTable(const NYPath::TYPath& path);

    static void SyncUnmountTable(const NYPath::TYPath& path);

    static std::tuple<TSharedRange<NTableClient::TUnversionedRow>, NTableClient::TNameTablePtr> PrepareUnversionedRow(
        const std::vector<TString>& names,
        const TString& rowString);

    static void WriteUnversionedRow(
        std::vector<TString> names,
        const TString& rowString,
        const NApi::IClientPtr& client = Client_);

    static void WriteRows(
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

    static void WriteRows(
        NTableClient::TNameTablePtr nameTable,
        TSharedRange<NTableClient::TVersionedRow> rows,
        const NApi::IClientPtr& client = Client_);

private:
    static void RemoveSystemObjects(const NYPath::TYPath& path);

    static void RemoveTabletCells(
        std::function<bool(const TString&)> filter = [] (const TString&) { return true; });
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCppTests
} // namespace NYT
