#pragma once

#include "private.h"

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
        int id,
        i64 indexBlockSize);

    ~TChangeLog();

    //! Opens existing changelog.
    //! Throws exception on failure.
    void Open();
    //! Creates new changelog.
    //! Throws exception on failure.
    void Create(int prevRecordCount, const TEpochId& epoch);
    //! Finalizes current changelog.
    void Finalize();
    //! Reverts the effects of #FInalize, that is marks a finalize changelog as opened.
    //! Debug method, use it with care.
    void Definalize();

    //! Appends records to the changelog.
    void Append(int firstRecordId, const std::vector<TSharedRef>& records);
    //! Flushes the changelog.
    void Flush();
    //! Reads #recordCount records starting from record with id #firstRecordId.
    void Read(int firstRecordId, int recordCount, std::vector<TSharedRef>* result);
    //! Deletes all records with id greater or equal than #atRecordId.
    void Truncate(int truncatedRecordCount);

    int GetId() const;
    int GetPrevRecordCount() const;
    int GetRecordCount() const;
    const TEpochId& GetEpoch() const;
    bool IsFinalized() const;

private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
