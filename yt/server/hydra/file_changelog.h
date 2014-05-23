#pragma once

#include "public.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

IChangelogPtr CreateFileChangelog(
    const Stroka& path,
    const TSharedRef& meta,
    TFileChangelogConfigPtr config);

IChangelogPtr OpenFileChangelog(
    const Stroka& path,
    TFileChangelogConfigPtr config);

IChangelogStorePtr CreateFileChangelogStore(
    const TCellGuid& cellGuid,
    TFileChangelogStoreConfigPtr config);

// TODO(babenko): get rid of this
void ShutdownChangelogs();

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
