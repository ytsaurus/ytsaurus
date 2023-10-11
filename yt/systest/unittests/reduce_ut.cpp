
#include <library/cpp/yt/logging/logger.h>
#include <yt/systest/unittests/stub_dataset.h>

#include <yt/systest/operation/reduce.h>
#include <yt/systest/reduce_dataset.h>
#include <yt/systest/sort_dataset.h>
#include <yt/systest/table.h>
#include <yt/systest/unittests/util.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTest {
namespace {

class TReduceTest : public ::testing::Test
{
};

TEST_F(TReduceTest, SortReduce)
{
    TTable table{{
        TDataColumn{
            "a",
            NProto::EColumnType::ELatinString100,
            std::nullopt
        },
        TDataColumn{
            "b",
            NProto::EColumnType::ELatinString100,
            std::nullopt
        },
        TDataColumn{
            "c",
            NProto::EColumnType::EInt16,
            std::nullopt
        }
    },
    {}  // DeletedColumnNames
    };

    std::vector<std::vector<TNode>> data{
        {"x", "x", 10},
        {"x", "y", 15},
        {"x", "x", 10},
        {"z", "a", 12},
        {"z", "b", 15},
        {"z", "a", 10},
    };

    TStubDataset dataset(table, data);

    TSortOperation sortOperation{
        {"a", "b"}
    };

    TSortDataset sortDataset(dataset, sortOperation);

    std::vector<std::vector<TNode>> expectedSorted{
        {"x", "x", 10},
        {"x", "x", 10},
        {"x", "y", 15},
        {"z", "a", 12},
        {"z", "a", 10},
        {"z", "b", 15}
    };

    auto iterator = sortDataset.NewIterator();
    ExpectEqual(expectedSorted, iterator.get());

    TReduceOperation operation{
        std::make_unique<TSumReducer>(table, 2, TDataColumn{"sum", NProto::EColumnType::EInt64, std::nullopt}),
        {"a", "b"},
    };
    TReduceDataset reducedDataset(sortDataset, operation);
    iterator = reducedDataset.NewIterator();

    std::vector<std::vector<TNode>> expected{
        {"x", "x", 20},
        {"x", "y", 15},
        {"z", "a", 22},
        {"z", "b", 15},
    };
    ExpectEqual(expected, iterator.get());
}

}  // namespace
}  // namespace NYT::NTest
