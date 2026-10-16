#pragma once

//! Umbrella header for process-function unit tests: pulls in the full test harness (entity
//! builders, output collector, runtime-context builder, state environment and epoch driver).
//! Include this instead of the individual harness headers.

#include "entity_builders.h"
#include "process_function_test_harness.h"
#include "recording_output_collector.h"
#include "test_runtime_context.h"
#include "test_state_environment.h"
