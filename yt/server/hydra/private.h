#pragma once

#include "public.h"

#include <core/misc/lazy_ptr.h>

#include <ytlib/hydra/private.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

class TSyncFileChangelog;
typedef TIntrusivePtr<TSyncFileChangelog> TSyncFileChangelogPtr;

class TResponseKeeper;
typedef TIntrusivePtr<TResponseKeeper> TResponseKeeperPtr;

class TDecoratedAutomaton;
typedef TIntrusivePtr<TDecoratedAutomaton> TDecoratedAutomatonPtr;

class TLeaderRecovery;
typedef TIntrusivePtr<TLeaderRecovery> TLeaderRecoveryPtr;

class TFollowerRecovery;
typedef TIntrusivePtr<TFollowerRecovery> TFollowerRecoveryPtr;

class TFollowerTracker;
typedef TIntrusivePtr<TFollowerTracker> TFollowerTrackerPtr;

class TLeaderCommitter;
typedef TIntrusivePtr<TLeaderCommitter> TLeaderCommitterPtr;

class TFollowerCommitter;
typedef TIntrusivePtr<TFollowerCommitter> TFollowerCommitterPtr;

class TChangelogRotation;
typedef TIntrusivePtr<TChangelogRotation> TChangelogRotationPtr;

struct TSnapshotInfo;

////////////////////////////////////////////////////////////////////////////////

//! A special value indicating that the number of records in the previous
//! changelog is undetermined since there is no previous changelog.
const int NonexistingPrevRecordCount = -1;

//! A special value indicating that the number of records in the previous changelog
//! is unknown.
const int UnknownPrevRecordCount = -2;

//! A special value representing an invalid snapshot (or changelog) id.
const int NonexistingSegmentId = -1;

extern const Stroka LogSuffix;
extern const Stroka IndexSuffix;
extern const Stroka MultiplexedDirectory;
extern const Stroka SplitSuffix;
extern const Stroka CleanSuffix;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
