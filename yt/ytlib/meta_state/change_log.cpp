#include "stdafx.h"
#include "change_log.h"
#include "change_log_impl.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

TChangeLog::TChangeLog(
    const Stroka& fileName,
    int id,
    i64 indexBlockSize)
    : Impl(new TImpl(
        fileName,
        id,
        indexBlockSize))
{ }

TChangeLog::~TChangeLog()
{ }

int TChangeLog::GetId() const
{
    return Impl->GetId();
}

int TChangeLog::GetPrevRecordCount() const
{
    return Impl->GetPrevRecordCount();
}

int TChangeLog::GetRecordCount() const
{
    return Impl->GetRecordCount();
}

const TEpochId& TChangeLog::GetEpoch() const
{
    return Impl->GetEpoch();
}

bool TChangeLog::IsFinalized() const
{
    return Impl->IsFinalized();
}

void TChangeLog::Open()
{
    Impl->Open();
}

void TChangeLog::Create(int prevRecordCount, const TEpochId& epoch)
{
    Impl->Create(prevRecordCount, epoch);
}

void TChangeLog::Finalize()
{
    Impl->Finalize();
}

void TChangeLog::Definalize()
{
    Impl->Definalize();
}

void TChangeLog::Append(int firstRecordId, const std::vector<TSharedRef>& records)
{
    Impl->Append(firstRecordId, records);
}

void TChangeLog::Flush()
{
    Impl->Flush();
}

void TChangeLog::Read(int firstRecordId, int recordCount, std::vector<TSharedRef>* result)
{
    Impl->Read(firstRecordId, recordCount, result);
}

void TChangeLog::Truncate(int truncatedRecordCount)
{
    Impl->Truncate(truncatedRecordCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
