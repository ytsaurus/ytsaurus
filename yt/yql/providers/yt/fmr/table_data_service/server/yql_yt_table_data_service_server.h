#include <yt/yql/providers/yt/fmr/table_data_service/interface/yql_yt_table_data_service.h>
#include <yql/essentials/utils/runnable.h>

namespace NYql::NFmr {

using IFmrServer = IRunnable;

struct TTableDataServiceServerSettings {
    ui64 WorkerId;
    ui64 WorkersNum;
    TString Host = "localhost";
    ui16 Port = 7000;
};

IFmrServer::TPtr MakeTableDataServiceServer(const TTableDataServiceServerSettings& settings);

} // namespace NYql::NFmr
