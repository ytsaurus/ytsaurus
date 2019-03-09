#pragma once

#include "api_test_base.h"


namespace NYT {
namespace NCppTests {

////////////////////////////////////////////////////////////////////////////////

class TModifyRowsTest
    : public TDynamicTablesTestBase
{
public:
    void SetUp() override;
    void TearDown() override;

    static void SetUpTestCase();

protected:
    static NApi::ITransactionPtr Transaction_;
    static THashSet<i64> Keys_;

    static void WriteSimpleRow(
        i64 key,
        i64 value,
        std::optional<i64> sequenceNumber = std::nullopt);

    static void SyncCommit();

    static void CheckTableContents(
        const std::vector<std::pair<i64, i64>>& simpleRows);

private:
    static TString MakeRowString(i64 key, i64 value);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCppTests
} // namespace NYT

