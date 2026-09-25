#include "config.h"

namespace NYT::NNbd::NDynamicTable {

////////////////////////////////////////////////////////////////////////////////

void TDynamicTableBlockDeviceConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("size", &TThis::Size)
        .GreaterThanOrEqual(0);
    registrar.Parameter("block_size", &TThis::BlockSize)
        .GreaterThanOrEqual(0);
    registrar.Parameter("read_batch_size", &TThis::ReadBatchSize)
        .Default(16).GreaterThan(0);
    registrar.Parameter("write_batch_size", &TThis::WriteBatchSize)
        .Default(16).GreaterThan(0);
    registrar.Parameter("table_path", &TThis::TablePath);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNbd::NDynamicTable
