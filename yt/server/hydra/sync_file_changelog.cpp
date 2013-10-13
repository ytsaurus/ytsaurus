#include "stdafx.h"
#include "sync_file_changelog.h"
#include "sync_file_changelog_impl.h"
#include "config.h"

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TSyncFileChangelog::TSyncFileChangelog(
    const Stroka& fileName,
    int id,
    TFileChangelogConfigPtr config)
    : Impl(new TImpl(
        fileName,
        id,
        config))
{ }

TSyncFileChangelog::~TSyncFileChangelog()
{ }

TFileChangelogConfigPtr TSyncFileChangelog::GetConfig()
{
    return Impl->GetConfig();
}

void TSyncFileChangelog::Open()
{
    Impl->Open();
}

void TSyncFileChangelog::Close()
{
    Impl->Close();
}

void TSyncFileChangelog::Create(const TChangelogCreateParams& params)
{
    Impl->Create(params);
}

int TSyncFileChangelog::GetId() const
{
    return Impl->GetId();
}

int TSyncFileChangelog::GetRecordCount() const
{
    return Impl->GetRecordCount();
}

int TSyncFileChangelog::GetPrevRecordCount() const
{
    return Impl->GetPrevRecordCount();
}

bool TSyncFileChangelog::IsSealed() const
{
    return Impl->IsSealed();
}

void TSyncFileChangelog::Append(
    int firstRecordId,
    const std::vector<TSharedRef>& records)
{
    Impl->Append(firstRecordId, records);
}

void TSyncFileChangelog::Flush()
{
    Impl->Flush();
}

TInstant TSyncFileChangelog::GetLastFlushed()
{
    return Impl->GetLastFlushed();
}

std::vector<TSharedRef> TSyncFileChangelog::Read(
    int firstRecordId,
    int maxRecords,
    i64 maxBytes)
{
    return Impl->Read(firstRecordId, maxRecords, maxBytes);
}

void TSyncFileChangelog::Seal(int recordCount)
{
    Impl->Seal(recordCount);
}

void TSyncFileChangelog::Unseal()
{
    Impl->Unseal();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
