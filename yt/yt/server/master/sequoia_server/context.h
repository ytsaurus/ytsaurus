#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/ytlib/sequoia_client/write_set.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NSequoiaServer {

////////////////////////////////////////////////////////////////////////////////

struct ISequoiaContext
{
    virtual ~ISequoiaContext() = default;

    virtual void WriteRow(
        NSequoiaClient::ESequoiaTable table,
        NTableClient::TUnversionedRow row) = 0;

    template <class TRow>
    void WriteRow(const TRow& row);

    virtual void DeleteRow(
        NSequoiaClient::ESequoiaTable table,
        NTableClient::TLegacyKey key) = 0;

    template <class TRow>
    void DeleteRow(const TRow& row);

    virtual void SubmitRows() noexcept = 0;

    virtual const NTableClient::TRowBufferPtr& GetRowBuffer() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

ISequoiaContext* GetSequoiaContext();

////////////////////////////////////////////////////////////////////////////////

class TSequoiaContextGuard
    : public TNonCopyable
{
public:
    TSequoiaContextGuard(
        NCellMaster::TBootstrap* bootstrap,
        NTransactionServer::TTransactionId transactionId,
        const NSequoiaClient::NProto::TWriteSet& protoWriteSet);
    ~TSequoiaContextGuard();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer

#define CONTEXT_INL_H_
#include "context-inl.h"
#undef CONTEXT_INL_H_
