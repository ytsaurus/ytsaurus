#include <library/cpp/threading/future/future.h>
#include <yt/yql/providers/yt/fmr/table_data_service/interface/table_data_service.h>

namespace NYql {

struct TLocalTableDataServiceSettings {
    ui32 NumParts;
};

ITableDataService::TPtr MakeLocalTableDataService(const TLocalTableDataServiceSettings& settings);

} // namspace NYql
