#include "stdafx.h"
#include "sync_file_changelog.h"
#include "sync_file_changelog_impl.h"
#include "config.h"

#include <core/misc/fs.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

TSyncFileChangelog::TSyncFileChangelog(
    const Stroka& fileName,
    TFileChangelogConfigPtr config)
    : Impl_(new TImpl(
        fileName,
        config))
{ }

TSyncFileChangelog::~TSyncFileChangelog()
{ }

TFileChangelogConfigPtr TSyncFileChangelog::GetConfig()
{
    return Impl_->GetConfig();
}

const Stroka& TSyncFileChangelog::GetFileName() const
{
    return Impl_->GetFileName();
}

void TSyncFileChangelog::Open()
{
    Impl_->Open();
}

void TSyncFileChangelog::Close()
{
    Impl_->Close();
}

void TSyncFileChangelog::Create(const TSharedRef& meta)
{
    Impl_->Create(meta);
}

int TSyncFileChangelog::GetRecordCount() const
{
    return Impl_->GetRecordCount();
}

i64 TSyncFileChangelog::GetDataSize() const
{
    return Impl_->GetDataSize();
}

TSharedRef TSyncFileChangelog::GetMeta() const
{
    return Impl_->GetMeta();
}

bool TSyncFileChangelog::IsSealed() const
{
    return Impl_->IsSealed();
}

void TSyncFileChangelog::Append(
    int firstRecordId,
    const std::vector<TSharedRef>& records)
{
    Impl_->Append(firstRecordId, records);
}

void TSyncFileChangelog::Flush()
{
    Impl_->Flush();
}

TInstant TSyncFileChangelog::GetLastFlushed()
{
    return Impl_->GetLastFlushed();
}

std::vector<TSharedRef> TSyncFileChangelog::Read(
    int firstRecordId,
    int maxRecords,
    i64 maxBytes)
{
    return Impl_->Read(firstRecordId, maxRecords, maxBytes);
}

void TSyncFileChangelog::Seal(int recordCount)
{
    Impl_->Seal(recordCount);
}

void TSyncFileChangelog::Unseal()
{
    Impl_->Unseal();
}

////////////////////////////////////////////////////////////////////////////////

void RemoveChangelogFiles(const Stroka& dataFileName)
{
    NFS::Remove(dataFileName);

    auto indexFileName = dataFileName + "." + ChangelogIndexExtension;
    if (NFS::Exists(indexFileName)) {
        NFS::Remove(indexFileName);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
