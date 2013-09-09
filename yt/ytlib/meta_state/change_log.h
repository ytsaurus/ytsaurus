#pragma once

#include "private.h"

#include <core/misc/ref.h>

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
    //! Throws an exception on failure.
    void Open();

    //! Creates new changelog.
    //! Throws an exception on failure.
    void Create(int prevRecordCount, const TEpochId& epoch);

    //! Finalizes current changelog.
    void Finalize();

    //! Reverts the effects of #Finalize, that is marks a finalize changelog as opened.
    //! Debug method, use it with care.
    void Definalize();

    //! Appends records to the changelog.
    void Append(int firstRecordId, const std::vector<TSharedRef>& records);

    //! Flushes the changelog.
    void Flush();

    //! Reads #recordCount records starting from record with id #firstRecordId.
    //! Stops if more than #maxSizes bytes are read.
    void Read(
        int firstRecordId,
        int recordCount,
        i64 maxSize,
        std::vector<TSharedRef>* result);

    //! Deletes all records with id greater or equal than #atRecordId.
    void Truncate(int truncatedRecordCount);

    int GetId() const;
    int GetPrevRecordCount() const;
    int GetRecordCount() const;
    const TEpochId& GetEpoch() const;
    bool IsFinalized() const;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
