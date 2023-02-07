#pragma once

class IOutputStream;

namespace NMR {
class TServer;
} // namesapce NMR


namespace NYT {
namespace NTestUtil {

void PrintTable(NMR::TServer& server, const char* tableName, IOutputStream& out);

} // namespace NTestUtil
} // namespace NYT
