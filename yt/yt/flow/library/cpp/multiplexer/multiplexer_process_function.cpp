#include "multiplexer_process_function.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TEmptyMultiplexerUserState::Register(TRegistrar /*registrar*/)
{ }

void TMultiplexerKeyState::Register(TRegistrar registrar)
{
    registrar.Parameter("is_active", &TThis::IsActive)
        .Default(false);
    registrar.Parameter("offset", &TThis::Offset)
        .Default();
    registrar.Parameter("initial_start_offset", &TThis::InitialStartOffset)
        .Default();
    registrar.Parameter("in_second_phase", &TThis::InSecondPhase)
        .Default(false);
    registrar.Parameter("offset_schema", &TThis::OffsetSchema)
        .Default();
}

void TDynamicMultiplexerParameters::Register(TRegistrar registrar)
{
    registrar.Parameter("timer_period", &TThis::TimerPeriod)
        .Default(TDuration::Seconds(5))
        .GreaterThan(TDuration::Seconds(1));
    registrar.Parameter("batch_size", &TThis::BatchSize)
        .Default(1000)
        .GreaterThan(0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
