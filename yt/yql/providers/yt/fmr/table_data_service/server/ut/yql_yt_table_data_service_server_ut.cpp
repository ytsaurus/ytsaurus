#include <library/cpp/threading/future/wait/wait.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/stream/file.h>
#include <util/system/tempfile.h>
#include <yt/yql/providers/yt/fmr/test_tools/table_data_service/yql_yt_table_data_service_helpers.h>

namespace NYql::NFmr {

const TString Group = "table_id_part_id:";
const TString ChunkId = "0";

Y_UNIT_TEST_SUITE(TableDataServiceWorkerTests) {
    Y_UNIT_TEST(SendGetRequestNonExistentKey) {
        TPortManager pm;
        const ui16 port = pm.GetPort();
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);

        auto gottenTableContent = tableDataServiceClient->Get(Group, ChunkId).GetValueSync();
        UNIT_ASSERT(!gottenTableContent);
    }
    Y_UNIT_TEST(SendGetRequestExistingKey) {
        TPortManager pm;
        const ui16 port = pm.GetPort();
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);

        TString tableContent = "test_content";
        tableDataServiceClient->Put(Group, ChunkId, tableContent).Wait();
        auto gottenTableContent = tableDataServiceClient->Get(Group, ChunkId).GetValueSync();
        UNIT_ASSERT(gottenTableContent);
        UNIT_ASSERT_NO_DIFF(*gottenTableContent, tableContent);
    }
    Y_UNIT_TEST(SendDeleteRequestExistingKey) {
        TPortManager pm;
        const ui16 port = pm.GetPort();
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);

        TString tableContent = "test_content";
        tableDataServiceClient->Put(Group, ChunkId, tableContent).Wait();
        tableDataServiceClient->Delete(Group, ChunkId).Wait();
        Sleep(TDuration::Seconds(2)); // future returns only when deletion is registered, not completed, so have to sleep
        auto gottenTableContent = tableDataServiceClient->Get(Group, ChunkId).GetValueSync();
        UNIT_ASSERT(!gottenTableContent);
    }
    Y_UNIT_TEST(SeveralTableDataSerivceServerNodes) {
        ui64 workersNum = 10;
        std::vector<IFmrServer::TPtr> tableDataServiceServers;
        TPortManager pm;
        const ui16 port = pm.GetPort();
        for (size_t i = 0; i < workersNum; ++i) {
            auto tableDataServiceWorkerSettings = TTableDataServiceServerSettings{
                .WorkerId = i, .WorkersNum = workersNum, .Host = "localhost", .Port = static_cast<ui16>(port + i)
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
        TString tableContent = "test_content";
        std::vector<NThreading::TFuture<void>> putFutures;
        for (size_t i = 0; i < workersNum; ++i) {
            auto curGroup = Group + ToString(i), curChunkId = ChunkId + ToString(i);
            putFutures.emplace_back(tableDataServiceClient->Put(curGroup, curChunkId, tableContent + ToString(i)));
        }
        NThreading::WaitAll(putFutures).Wait();
        for (size_t i = 0; i < workersNum; ++i) {
            auto curGroup = Group + ToString(i), curChunkId = ChunkId + ToString(i);
            auto gottenTableContent = tableDataServiceClient->Get(curGroup, curChunkId).GetValueSync();
            UNIT_ASSERT(gottenTableContent);
            UNIT_ASSERT_NO_DIFF(*gottenTableContent, tableContent + ToString(i));
        }
    }
    Y_UNIT_TEST(RegisterDeletion) {
        TPortManager pm;
        const ui16 port = pm.GetPort();
        auto tableDataServiceServer = MakeTableDataServiceServer(port);
        auto tableDataServiceClient = MakeTableDataServiceClient(port);

        TString content = "test_content";
        ui64 keysNum = 10000;
        for (ui64 i = 0; i < keysNum; ++i) {
            TString chunkId = ToString(i);
            tableDataServiceClient->Put(Group, chunkId, content +  ToString(i)).GetValueSync();
        }
        tableDataServiceClient->RegisterDeletion({Group}).GetValueSync();

        for (ui64 i = 0; i < keysNum; ++i) {
            UNIT_ASSERT(!tableDataServiceClient->Get(Group, ToString(i)).GetValueSync());
        }
    }
}

} // namespace NYql::NFmr
