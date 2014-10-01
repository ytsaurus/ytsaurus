#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <ytlib/election/public.h>

#include <server/hydra/mutation.h>

namespace NYT {
namespace NCellMaster {

////////////////////////////////////////////////////////////////////////////////

class TWorldInitializer
    : public TRefCounted
{
public:
    TWorldInitializer(
        TCellMasterConfigPtr config,
        TBootstrap* bootstrap);
    ~TWorldInitializer();

    //! Checks if the cell is initialized.
    bool IsInitialized();

    //! Same as #IsInitialized but throws on failure.
    void ValidateInitialized();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TWorldInitializer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellMaster
} // namespace NYT
