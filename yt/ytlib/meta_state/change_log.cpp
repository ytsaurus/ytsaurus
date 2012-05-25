#include "stdafx.h"
#include "change_log.h"
#include "change_log_impl.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

TChangeLog::TChangeLog(
    const Stroka& fileName,
    i32 id,
    i64 indexBlockSize)
    : Impl(new TImpl(
        fileName,
        id,
        indexBlockSize))
{ }

TChangeLog::~TChangeLog()
{ }

i32 TChangeLog::GetId() const
{
    return Impl->GetId();
}

i32 TChangeLog::GetPrevRecordCount() const
{
    return Impl->GetPrevRecordCount();
}

i32 TChangeLog::GetRecordCount() const
{
    return Impl->GetRecordCount();
}

bool TChangeLog::IsFinalized() const
{
    return Impl->IsFinalized();
}

void TChangeLog::Open()
{
    Impl->Open();
}

void TChangeLog::Create(i32 prevRecordCount)
{
    Impl->Create(prevRecordCount);
}

void TChangeLog::Finalize()
{
    Impl->Finalize();
}

void TChangeLog::Append(i32 firstRecordId, const std::vector<TSharedRef>& records)
{
    Impl->Append(firstRecordId, records);
}

void TChangeLog::Flush()
{
    Impl->Flush();
}

void TChangeLog::Read(i32 firstRecordId, i32 recordCount, std::vector<TSharedRef>* result)
{
    Impl->Read(firstRecordId, recordCount, result);
}

void TChangeLog::Truncate(i32 atRecordId)
{
    Impl->Truncate(atRecordId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
