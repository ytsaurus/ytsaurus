#include "jobs.h"

namespace NYT::NTesting {

////////////////////////////////////////////////////////////////////////////////

REGISTER_MAPPER(TAlwaysFailingMapper);
REGISTER_MAPPER(TIdMapper);
REGISTER_REDUCER(TIdReducer);
REGISTER_MAPPER(TSleepingMapper);
REGISTER_MAPPER(THugeStderrMapper);
REGISTER_MAPPER(TUrlRowIdMapper);
REGISTER_MAPPER(TYdlUrlRowIdMapper);
REGISTER_REDUCER(TUrlRowIdReducer);
REGISTER_REDUCER(TYdlUrlRowIdReducer);
REGISTER_MAPPER(TMapperThatWritesStderr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTesting
