#include "change_log_writer.h"

#include "../actions/action_util.h"
#include "../logging/log.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChangeLogWriter");
static i32 UnflushedThreshold = 1 << 20;

////////////////////////////////////////////////////////////////////////////////

class TChangeLogWriter::TImpl
    : public TActionQueue
{
public:
    void Append(
        i32 recordId,
        TSharedRef data,
        TChangeLog::TPtr changeLog,
        TAppendResult::TPtr result)
    {
        TChangeLogEntryMap::iterator it = ChangeLogEntryMap.find(changeLog);
        TChangeLogEntry::TPtr entry;
        if (it == ChangeLogEntryMap.end()) {
            entry = new TChangeLogEntry(changeLog);
            it = ChangeLogEntryMap.insert(MakePair(changeLog, entry)).first;
        } else {
            entry = it->second;
        }

        if (entry->Append(recordId, data, result)) {
            ChangeLogEntryMap.erase(it);
        }
    }

    TVoid Finalize(TChangeLog::TPtr changeLog)
    {
        TChangeLogEntryMap::iterator it = ChangeLogEntryMap.find(changeLog);
        if (it != ChangeLogEntryMap.end()) {
            TChangeLogEntry::TPtr entry = it->second;
            entry->Flush();
            ChangeLogEntryMap.erase(it);
        }
        changeLog->Finalize();
        return TVoid();
    }

private:
    class TChangeLogEntry
        : public TRefCountedBase
    {

        TChangeLog::TPtr ChangeLog;
        i32 UnflushedSize;

        typedef yvector<TChangeLogWriter::TAppendResult::TPtr> TAppendResults;
        TAppendResults Results;

    public:
        typedef TIntrusivePtr<TChangeLogEntry> TPtr;

        TChangeLogEntry(TChangeLog::TPtr changeLog)
            : ChangeLog(changeLog)
            , UnflushedSize(0)
        { }

        // Returns true if flushed.
        bool Append(
            i32 recordId,
            TSharedRef data,
            TAppendResult::TPtr result)
        {
            ChangeLog->Append(recordId, data);
            Results.push_back(result);
            UnflushedSize += ((TRef) data).Size();
            if (UnflushedSize >= UnflushedThreshold) {
                Flush();
                return true;
            }
            return false;
        }

        void Flush()
        {
            ChangeLog->Flush();
            UnflushedSize = 0;
            for(i32 i = 0; i < Results.ysize(); ++i) {
                Results[i]->Set(TChangeLogWriter::OK);
            }
            Results.clear();
        }
    };

    virtual void OnIdle()
    {
        for (TChangeLogEntryMap::iterator it = ChangeLogEntryMap.begin();
            it != ChangeLogEntryMap.end(); ++it)
        {
            it->second->Flush();
        }
        ChangeLogEntryMap.clear();
    }

    typedef yhash_map<TChangeLog::TPtr,
        TChangeLogEntry::TPtr,
        TIntrusivePtrHash<TChangeLog> > TChangeLogEntryMap;
    TChangeLogEntryMap ChangeLogEntryMap;
};

////////////////////////////////////////////////////////////////////////////////

TChangeLogWriter::TChangeLogWriter(TChangeLog::TPtr changeLog)
    : ChangeLog(changeLog)
    , Impl(RefCountedSingleton<TImpl>())
{ }


TChangeLogWriter::~TChangeLogWriter()
{ }


TChangeLogWriter::TAppendResult::TPtr TChangeLogWriter::Append(
    i32 recordId,
    const TSharedRef& changeData)
{
    TAppendResult::TPtr result = new TAppendResult();
    Impl->Invoke(FromMethod(
        &TImpl::Append,
        Impl,
        recordId,
        changeData,
        ChangeLog,
        result));
    return result;
}

void TChangeLogWriter::Close()
{
    FromMethod(&TImpl::Finalize, Impl, ChangeLog)
        ->AsyncVia(~Impl)
        ->Do()
        ->Get();
    LOG_INFO("Changelog %d was closed", ChangeLog->GetId());
}

TChangeLog::TPtr TChangeLogWriter::GetChangeLog() const
{
    return ChangeLog;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

