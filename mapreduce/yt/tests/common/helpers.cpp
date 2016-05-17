#include <mapreduce/interface/all.h>

#include <util/stream/output.h>


namespace NYT {
namespace NCommonTest {

using namespace NMR;

void PrintTable(TServer& server, const char* tableName, TOutputStream& out) {
    TClient client(server);
    TTable table(client, tableName);
    for (auto&& it = table.Begin(); it != table.End(); ++it) {
        out << "'" << it.GetKey().AsString() << "'"
            << "\t'" << it.GetSubKey().AsString() << "'"
            << "\t'" << it.GetValue().AsString() << "'"
            << "\n";
    }
}

} // namespace NCommonTest
} // namespace NYT
