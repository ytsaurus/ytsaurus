#pragma once

class TOutputStream;

namespace NMR {
class TServer;
} // namesapce NMR


namespace NYT {
namespace NCommonTest {

void PrintTable(NMR::TServer& server, const char* tableName, TOutputStream& out);

} // namespace NCommonTest
} // namespace NYT
