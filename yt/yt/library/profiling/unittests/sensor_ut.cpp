#include <gtest/gtest.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/impl.h>

#include <yt/yt/library/profiling/solomon/registry.h>

namespace NYT::NProfiling {
namespace {

////////////////////////////////////////////////////////////////////////////////

TRegistry EventQueue{"/event_queue"};

auto GlobalEventCounter = EventQueue.Counter("/event_counter");
auto GlobalQueueSize = EventQueue.Gauge("/queue_size");

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NProfiling
