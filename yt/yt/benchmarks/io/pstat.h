#pragma once

#include <library/cpp/yt/misc/enum.h>

#include <util/generic/fwd.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

using TProcessId = TString;

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> ReadProcessStat(TProcessId processId);

std::vector<TProcessId> FindProcessIds(TString command);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EProcessStatField,
    ((Pid)                           (0))
    ((Command)                       (1))
    ((State)                         (2))
    ((Ppid)                          (3))
    ((Pgrp)                          (4))
    ((Session)                       (5))
    ((TtyNr)                         (6))
    ((Tpidgr)                        (7))
    ((Flags)                         (8))
    ((MinorPageFaults)               (9))
    ((ChildrenMinorPageFaults)       (10))
    ((MajorPageFaults)               (11))
    ((ChildrenMajorPageFaults)       (12))
    ((UserTime)                      (13))
    ((SystemTime)                    (14))
    ((ChildrenUserTime)              (15))
    ((ChildrenSystemTime)            (16))
    ((Priority)                      (17))
    ((Nice)                          (18))
    ((NumThreads)                    (19))
    ((Itervalue)                     (20))
    ((StartTime)                     (21))
    ((VirtualMemorySize)             (22))
    ((ResidentSetSize)               (23))
    ((ResidentSetSizeLimit)          (24))
    ((CodeStartAddress)              (25))
    ((CodeEndAddress)                (26))
    ((StackStartAddress)             (27))
    ((KernelStateESP)                (28))
    ((KernelStateEIP)                (29))
    ((PendingSignalMask)             (30))
    ((BlockedSignalMask)             (31))
    ((IgnoredSignalMask)             (32))
    ((CaughtSignalMask)              (33))
    ((Wchan)                         (34))
    ((PagesSwapped)                  (35))
    ((ChildrenPagesSwapped)          (36))
    ((ExitSignal)                    (37))
    ((LastExecutionCPU)              (38))
    ((RtPriority)                    (39))
    ((Policy)                        (40))
    ((BlockIoDelayTicks)             (41))
    ((GuestTime)                     (42))
    ((ChildrenGuestTime)             (43))
    ((DataStartAddress)              (44))
    ((DataEndAddress)                (45))
    ((BrkStartAddress)               (46))
    ((ArgumentsStartAddress)         (47))
    ((ArgumentsEndAddress)           (48))
    ((EnvironmentStartAddress)       (49))
    ((EnvironmentEndAddress)         (50))
    ((ExitCode)                      (51))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
