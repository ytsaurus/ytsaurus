#include "stdafx.h"
#include "remote_changelog_store.h"
#include "changelog.h"
#include "config.h"
#include "private.h"

#include <core/logging/tagged_logger.h>

namespace NYT {
namespace NHydra {

using namespace NApi;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

class TRemoteChangelogStore
    : public IChangelogStore
{
public:
    TRemoteChangelogStore(
        TRemoteChangelogStoreConfigPtr config,
        const TYPath& remotePath,
        IClientPtr masterClient)
        : Config_(config)
        , RemotePath_(remotePath)
        , MasterClient_(masterClient)
        , Logger(HydraLogger)
    {
        Logger.AddTag(Sprintf("Path: %s", ~RemotePath_));
    }

    virtual const TCellGuid& GetCellGuid() const override
    {
        YUNIMPLEMENTED();
    }

    virtual IChangelogPtr CreateChangelog(
        int id,
        const TSharedRef& meta) override
    {
        YUNIMPLEMENTED();
    }

    virtual IChangelogPtr TryOpenChangelog(int id) override
    {
        YUNIMPLEMENTED();
    }

    virtual int GetLatestChangelogId(int initialId) override
    {
        YUNIMPLEMENTED();
    }

private:
    TRemoteChangelogStoreConfigPtr Config_;
    TYPath RemotePath_;
    IClientPtr MasterClient_;

    NLog::TTaggedLogger Logger;


    class TRemoteChangelog
        : public IChangelog
    {
    public:
        virtual TSharedRef GetMeta() const override
        {
            YUNIMPLEMENTED();
        }

        virtual int GetRecordCount() const override
        {
            YUNIMPLEMENTED();
        }

        virtual i64 GetDataSize() const override
        {
            YUNIMPLEMENTED();
        }

        virtual bool IsSealed() const override
        {
            YUNIMPLEMENTED();
        }

        virtual TAsyncError Append(const TSharedRef& data) override
        {
            YUNIMPLEMENTED();
        }

        virtual TAsyncError Flush() override
        {
            YUNIMPLEMENTED();
        }

        virtual void Close() override
        {
            YUNIMPLEMENTED();
        }

        virtual std::vector<TSharedRef> Read(
            int firstRecordId,
            int maxRecords,
            i64 maxBytes) const override
        {
            YUNIMPLEMENTED();
        }

        virtual TAsyncError Seal(int recordCount) override
        {
            YUNIMPLEMENTED();
        }

        virtual TAsyncError Unseal() override
        {
            YUNIMPLEMENTED();
        }

    };

};

IChangelogStorePtr CreateRemoteChangelogStore(
    TRemoteChangelogStoreConfigPtr config,
    const TYPath& remotePath,
    IClientPtr masterClient)
{
    return New<TRemoteChangelogStore>(
        config,
        remotePath,
        masterClient);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
