#include "kv.h"
#include <contrib/ydb/library/workload/abstract/workload_factory.h>

namespace NYdbWorkload {

TWorkloadFactory::TRegistrator<TKvWorkloadParams> KvRegistrar("kv");

}