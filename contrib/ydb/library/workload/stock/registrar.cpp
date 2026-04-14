#include "stock.h"
#include <contrib/ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TStockWorkloadParams> StockRegistrar("stock");

}
