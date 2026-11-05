#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

// Builds the ref-counted retention scenarios the analysis test inspects: a heap
// cycle, parked-fiber and running-thread holders, a final (counter-before-object)
// type, a TAtomicIntrusivePtr holder, a virtual diamond, and a strong/weak edge.
//
// Returns the one object that must be pinned ONLY by the running thread's stack;
// main() holds the returned strong ref as a local across the breakpoint, so that
// object has no heap/global holder and is attributable solely to main's stack.
NYT::TRefCountedPtr SetupGdbRefCountFixtures();
