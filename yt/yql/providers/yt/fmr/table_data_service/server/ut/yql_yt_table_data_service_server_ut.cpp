#include <library/cpp/threading/future/wait/wait.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/stream/file.h>
#include <util/system/tempfile.h>
#include <yt/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <yt/yql/providers/yt/fmr/test_tools/table_data_service/yql_yt_table_data_service_helpers.h>

namespace NYql::NFmr {

const TString Group = "table_id_part_id:";
const TString ChunkId = "0";
const TString Value = "test_content";

Y_UNIT_TEST_SUITE(TableDataServiceWorkerTests) {
    Y_UNIT_TEST(SendGetRequestNonExistentKey) {
        TPortManager pm;
        const ui16 port = pm.GetPort();
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            tableDataServiceClient->Get(Group, ChunkId).GetValueSync(),
            TFmrNonRetryableJobException,
            "Failed to get group table_id_part_id: and chunkId 0 from table data service"
        );
    }
    Y_UNIT_TEST(SendGetRequestExistingKey) {
        TPortManager pm;
        const ui16 port = pm.GetPort();
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);

        UNIT_ASSERT(tableDataServiceClient->Put(Group, ChunkId, Value).GetValueSync());
        auto gottenTableContent = tableDataServiceClient->Get(Group, ChunkId).GetValueSync();
        UNIT_ASSERT(gottenTableContent);
        UNIT_ASSERT_NO_DIFF(*gottenTableContent, Value);
    }
    Y_UNIT_TEST(SendDeleteRequestExistingKey) {
        TPortManager pm;
        const ui16 port = pm.GetPort();
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);

        UNIT_ASSERT(tableDataServiceClient->Put(Group, ChunkId, Value).GetValueSync());
        tableDataServiceClient->Delete(Group, ChunkId).Wait();
        Sleep(TDuration::Seconds(2)); // future returns only when deletion is registered, not completed, so have to sleep
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            tableDataServiceClient->Get(Group, ChunkId).GetValueSync(),
            TFmrNonRetryableJobException,
            "Failed to get group table_id_part_id: and chunkId 0 from table data service"
        );
    }
    Y_UNIT_TEST(SeveralTableDataSerivceServerNodes) {
        ui64 workersNum = 10;
        std::vector<IFmrServer::TPtr> tableDataServiceServers;
        TPortManager pm;
        const ui16 port = pm.GetPort();
        for (size_t i = 0; i < workersNum; ++i) {
            auto tableDataServiceWorkerSettings = TTableDataServiceServerSettings{
                .Host = "localhost", .Port = static_cast<ui16>(port + i)
            };
            auto tableDataServiceServer =  MakeTableDataServiceServer(MakeLocalTableDataService(), tableDataServiceWorkerSettings);
            tableDataServiceServer->Start();
            tableDataServiceServers.emplace_back(std::move(tableDataServiceServer));
        }

        std::vector<TTableDataServiceServerConnection> connections;
        for (size_t i = 0; i < workersNum; ++i) {
            connections.emplace_back("localhost", static_cast<ui16>(port + i));
        }
        TTempFileHandle hostsFile{};
        auto path = WriteHostsToFile(hostsFile, workersNum, connections);

        auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path=path});
        auto tableDataServiceClient = MakeTableDataServiceClient(tableDataServiceDiscovery);
        std::vector<NThreading::TFuture<bool>> putFutures;
        for (size_t i = 0; i < workersNum; ++i) {
            auto curGroup = Group + ToString(i), curChunkId = ChunkId + ToString(i);
            putFutures.emplace_back(tableDataServiceClient->Put(curGroup, curChunkId, Value + ToString(i)));
        }
        NThreading::WaitAll(putFutures).Wait();
        for (size_t i = 0; i < workersNum; ++i) {
            auto curGroup = Group + ToString(i), curChunkId = ChunkId + ToString(i);
            auto gottenTableContent = tableDataServiceClient->Get(curGroup, curChunkId).GetValueSync();
            UNIT_ASSERT(gottenTableContent);
            UNIT_ASSERT_NO_DIFF(*gottenTableContent, Value + ToString(i));
        }
    }
    Y_UNIT_TEST(RegisterDeletion) {
        TPortManager pm;
        const ui16 port = pm.GetPort();
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);

        ui64 keysNum = 1000;
        for (ui64 i = 0; i < keysNum; ++i) {
            TString chunkId = ToString(i);
            UNIT_ASSERT(tableDataServiceClient->Put(Group, chunkId, Value + ToString(i)).GetValueSync());
        }
        tableDataServiceClient->RegisterDeletion({Group}).GetValueSync();

        for (ui64 i = 0; i < keysNum; ++i) {
            TString expectedErrorMessage = "Failed to get group table_id_part_id: and chunkId " + ToString(i) + " from table data service";
            UNIT_ASSERT_EXCEPTION_CONTAINS(
                tableDataServiceClient->Get(Group, ToString(i)).GetValueSync(),
                TFmrNonRetryableJobException,
                expectedErrorMessage
            );
        }
    }
    Y_UNIT_TEST(Clear) {
        TPortManager pm;
        const ui16 port = pm.GetPort();
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);

        ui64 requestNum = 10;
        for (ui64 i = 0; i < requestNum; ++i) {
            for (ui64 j = 0; j < requestNum; ++j) {
                UNIT_ASSERT(tableDataServiceClient->Put(Group + ToString(i), ChunkId + ToString(j), "value" + ToString(i + j)).GetValueSync());
            }
        }
        tableDataServiceClient->Clear().GetValueSync();
        for (ui64 i = 0; i < requestNum; ++i) {
            for (ui64 j = 0; j < requestNum; ++j) {
                TString expectedErrorMessage = "Failed to get group table_id_part_id:" + ToString(i) + " and chunkId " + ChunkId + ToString(j) + " from table data service";
                UNIT_ASSERT_EXCEPTION_CONTAINS(
                    tableDataServiceClient->Get(Group + ToString(i), ChunkId + ToString(j)).GetValueSync(),
                    TFmrNonRetryableJobException,
                    expectedErrorMessage
                );
            }
        }
    }
}

} // namespace NYql::NFmr
