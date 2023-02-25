#include "table_printer.h"

#include <mapreduce/interface/all.h>

#include <util/stream/output.h>


namespace NYT {
namespace NTestUtil {

using namespace NMR;

void PrintTable(TServer& server, const char* tableName, IOutputStream& out) {
    TClient client(server);
    TTable table(client, tableName);
    if (table.IsEmpty()) {
        out << "Table is empty" << Endl;
    }
    for (auto&& it = table.Begin(); it != table.End(); ++it) {
        out << "'" << it.GetKey().AsString() << "'"
            << "\t'" << it.GetSubKey().AsString() << "'"
            << "\t'" << it.GetValue().AsString() << "'"
            << Endl;
    }
}

} // namespace NTestUtil
} // namespace NYT
