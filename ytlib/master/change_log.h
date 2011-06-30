#pragma once

#include "common.h"

#include "../misc/ptr.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TChangeLog
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TChangeLog> TPtr;

    TChangeLog(
        Stroka fileName,
        i32 id,
        i32 indexBlockSize = 1024 * 1024);

    i32 GetId() const;

    void Open();
    void Create(i32 prevRecordCount);
    void Finalize();

    void Append(i32 recordId, TRef recordData);
    void Flush();
    // TODO: get rid of dataHolder, turn recordData into yvector<TSharedRef>
    void Read(
        i32 startRecordId,
        i32 recordCount,
        TBlob* dataHolder,
        yvector<TRef>* recordData);
    void Truncate(i32 recordId);

    TMasterStateId GetPrevStateId() const;
    i32 GetRecordCount() const;
    bool IsFinalized() const;

private:
    struct TLogHeader;
    struct TRecordHeader;
    struct TLogIndexHeader;

    enum EState
    {
        S_Closed,
        S_Open,
        S_Finalized
    };

#pragma pack(push, 4)

    struct TLogIndexRecord
    {
        i32 RecordId;
        i32 Offset;

        bool operator<(const TLogIndexRecord& right) const;
    };

#pragma pack(pop)

    typedef yvector<TLogIndexRecord> TIndex;

    void HandleRecord(i32 recordId, i32 recordSize);
    TIndex::iterator GetLowerBound(i32 recordId);
    TIndex::iterator GetUpperBound(i32 recordId);

    void TruncateIndex(i32 indexRecordId);

    EState State;
    Stroka FileName;
    Stroka IndexFileName;
    i32 Id;
    i32 IndexBlockSize;

    i32 PrevRecordCount;
    i32 CurrentBlockSize;
    TAtomic CurrentFilePosition;
    i32 RecordCount;

    TIndex Index;
    TSpinLock IndexSpinLock;

    THolder<TFile> File;
    THolder<TBufferedFileOutput> FileOutput;
    THolder<TFile> IndexFile;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
