#pragma once

#include "public.h"

#include <ytlib/misc/ref.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

//! This class synchronously performs all operations on the changelog.
/*!
 * TChangeLog also tries to ensure correctness of all operations and
 * handle all unforeseen exceptional situations. More detailed explanation
 * of verifications and guarantees can be found in the member documentation.
 */
class TChangeLog
    : public TRefCounted
{
public:
    //! Basic constructor.
    TChangeLog(
        const Stroka& fileName,
        i32 id,
        bool disableFlush = false,
        i64 indexBlockSize = 1024 * 1024);

    ~TChangeLog();

    void Open();
    void Create(i32 prevRecordCount);
    void Finalize();

    void Append(i32 firstRecordId, const yvector<TSharedRef>& records);
    void Flush();
    void Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result);
    void Truncate(i32 atRecordId);

    i32 GetId() const;
    i32 GetPrevRecordCount() const;
    i32 GetRecordCount() const;
    bool IsFinalized() const;

private:
    class TImpl;

    THolder<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
