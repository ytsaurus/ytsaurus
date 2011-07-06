#pragma once

#include "common.h"

#include "../misc/ptr.h"

#include <util/generic/ptr.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! This class synchronously performs all operations on the changelog.
/*!
 * TChangeLog also tries to ensure correctness of all operations and
 * handle all unforeseen exceptional situations. More detailed explanation
 * of verifications and guarantees can be found in the member documentation.
 */
class TChangeLog
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChangeLog> TPtr;

    //! Basic constructor.
    TChangeLog(Stroka fileName, i32 id, i32 indexBlockSize = 1024 * 1024);

    void Open();
    void Create(i32 prevRecordCount);
    void Finalize();

    void Append(i32 recordId, TSharedRef recordData);
    void Flush();
    // TODO: get rid of dataHolder, turn recordData into yvector<TSharedRef>
    void Read(i32 firstRecordId, i32 recordCount, yvector<TSharedRef>* result);
    // TODO: argument name to firstRecordId?
    void Truncate(i32 recordId);

    i32 GetId() const;
    // TODO: Better name? ExpectedStateId?
    TMasterStateId GetPrevStateId() const;
    i32 GetRecordCount() const;
    bool IsFinalized() const;

private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
