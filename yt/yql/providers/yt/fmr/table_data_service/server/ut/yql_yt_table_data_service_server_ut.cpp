#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/wait/wait.h>
#include <util/stream/file.h>
#include <util/system/tempfile.h>
#include <yt/yql/providers/yt/fmr/table_data_service/client/yql_yt_table_data_service_client.h>
#include <yt/yql/providers/yt/fmr/table_data_service/discovery/file/yql_yt_file_service_discovery.h>
#include <yt/yql/providers/yt/fmr/table_data_service/server/yql_yt_table_data_service_server.h>

namespace NYql::NFmr {

TString WriteHostsToFile(TTempFileHandle& file, ui64 WorkersNum, const std::vector<TTableDataServiceServerConnection>& connections) {
    TString tempFileName = file.Name();
    TFileOutput writeHosts(file.Name());
    for (size_t i = 0; i < WorkersNum; ++i) {
        writeHosts.Write(TStringBuilder() << connections[i].Host << ":" << connections[i].Port << "\n");
    }
    return tempFileName;
}

Y_UNIT_TEST_SUITE(TableDataServiceWorkerTests) {
    Y_UNIT_TEST(SendGetRequestNonExistentKey) {
        ui16 port = 1200;
        TTableDataServiceServerSettings tableDataServiceWorkerSettings{.WorkerId = 0, .WorkersNum = 1, .Port = port};
        auto tableDataServiceServer = MakeTableDataServiceServer(tableDataServiceWorkerSettings);
        tableDataServiceServer->Start();

        TTempFileHandle hostsFile{};
        std::vector<TTableDataServiceServerConnection> connections{{.Host = "localhost", .Port = port}};
        ui64 workersNum = 1;
        auto path = WriteHostsToFile(hostsFile, workersNum, connections);

        auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path=path});

        auto tableDataServiceClient = MakeTableDataServiceClient(tableDataServiceDiscovery);
        TString tableId = "test_id", tableContent = "test_content";
        auto gottenTableContent = tableDataServiceClient->Get(tableId).GetValueSync();
        UNIT_ASSERT(!gottenTableContent);
    }
    Y_UNIT_TEST(SendGetRequestExistingKey) {
        ui16 port = 1201;
        TTableDataServiceServerSettings tableDataServiceWorkerSettings{.WorkerId = 0, .WorkersNum = 1, .Port = port};
        auto tableDataServiceServer = MakeTableDataServiceServer(tableDataServiceWorkerSettings);
        tableDataServiceServer->Start();

        TTempFileHandle hostsFile{};
        std::vector<TTableDataServiceServerConnection> connections{{.Host = "localhost", .Port = port}};
        ui64 workersNum = 1;
        auto path = WriteHostsToFile(hostsFile, workersNum, connections);

        auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path=path});
        auto tableDataServiceClient = MakeTableDataServiceClient(tableDataServiceDiscovery);
        TString tableId = "test_id", tableContent = "test_content";
        tableDataServiceClient->Put(tableId, tableContent).Wait();
        auto gottenTableContent = tableDataServiceClient->Get(tableId).GetValueSync();
        UNIT_ASSERT(gottenTableContent);
        UNIT_ASSERT_NO_DIFF(*gottenTableContent, tableContent);
    }
    Y_UNIT_TEST(SendDeleteRequestExistingKey) {
        ui16 port = 1202;
        TTableDataServiceServerSettings tableDataServiceWorkerSettings{.WorkerId = 0, .WorkersNum = 1, .Port = port};
        auto tableDataServiceServer = MakeTableDataServiceServer(tableDataServiceWorkerSettings);
        tableDataServiceServer->Start();

        TTempFileHandle hostsFile{};
        std::vector<TTableDataServiceServerConnection> connections{{.Host = "localhost", .Port = port}};
        ui64 workersNum = 1;
        auto path = WriteHostsToFile(hostsFile, workersNum, connections);

        auto tableDataServiceDiscovery = MakeFileTableDataServiceDiscovery({.Path=path});
        auto tableDataServiceClient = MakeTableDataServiceClient(tableDataServiceDiscovery);
        TString tableId = "test_id", tableContent = "test_content";
        tableDataServiceClient->Put(tableId, tableContent).Wait();
        tableDataServiceClient->Delete(tableId).Wait();
        auto gottenTableContent = tableDataServiceClient->Get(tableId).GetValueSync();
        UNIT_ASSERT(!gottenTableContent);
    }
    Y_UNIT_TEST(SeveralWorkers) {
        ui64 workersNum = 10;
        std::vector<IFmrServer::TPtr> tableDataServiceServers;
        ui16 port = 1203;
        for (size_t i = 0; i < workersNum; ++i) {
            auto tableDataServiceWorkerSettings = TTableDataServiceServerSettings{
                .WorkerId = i, .WorkersNum = workersNum, .Host = "localhost", .Port = static_cast<ui16>(port + i)
            };
            auto tableDataServiceServer = MakeTableDataServiceServer(tableDataServiceWorkerSettings);
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
        TString tableId = "test_id", tableContent = "test_content";
        std::vector<NThreading::TFuture<void>> putFutures;
        for (size_t i = 0; i < workersNum; ++i) {
            putFutures.emplace_back(tableDataServiceClient->Put(tableId + ToString(i), tableContent + ToString(i)));
        }
        NThreading::WaitAll(putFutures).Wait();
        for (size_t i = 0; i < workersNum; ++i) {
            auto gottenTableContent = tableDataServiceClient->Get(tableId + ToString(i)).GetValueSync();
            UNIT_ASSERT(gottenTableContent);
            UNIT_ASSERT_NO_DIFF(*gottenTableContent, tableContent + ToString(i));
        }
    }
}

} // namespace NYql::NFmr
