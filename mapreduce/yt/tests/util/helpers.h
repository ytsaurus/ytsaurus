#pragma once

class TOutputStream;

namespace NMR {
class TServer;
} // namesapce NMR


namespace NYT {
namespace NTestUtil {

void PrintTable(NMR::TServer& server, const char* tableName, TOutputStream& out);

} // namespace NTestUtil
} // namespace NYT
