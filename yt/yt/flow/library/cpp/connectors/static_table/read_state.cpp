#include "read_state.h"

namespace NYT::NFlow::NStaticTableConnector {

////////////////////////////////////////////////////////////////////////////////

TSystemTimestamp TReadRange::GetPlannedEndTimestamp() const
{
    return TSystemTimestamp(PlannedTimestamp.Underlying() + PlannedReadDuration.Seconds());
}

void TReadRange::Register(TRegistrar registrar)
{
    registrar.Parameter("begin_row_index", &TThis::BeginRowIndex);
    registrar.Parameter("end_row_index", &TThis::EndRowIndex);
    registrar.Parameter("planned_timestamp", &TThis::PlannedTimestamp);
    registrar.Parameter("planned_read_duration", &TThis::PlannedReadDuration);
}

////////////////////////////////////////////////////////////////////////////////

void TTableReadState::Register(TRegistrar registrar)
{
    registrar.Parameter("next_row_index", &TThis::NextRowIndex)
        .Default(0);
    registrar.Parameter("ranges", &TThis::Ranges)
        .Default();
    registrar.Parameter("free_slot_timestamps", &TThis::FreeSlotTimestamps)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NStaticTableConnector
