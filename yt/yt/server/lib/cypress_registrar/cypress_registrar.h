#pragma once

#include "public.h"

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/client/api/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

/*!
 *  The intended use of this class is to register multiple homogeneous instances
 *  in Cypress, where the path of each node is parametrized by corresponding
 *  instance address. Registrar will create nodes with appropriate expiration
 *  times and issue periodic pings. Orchid child node can be created on demand.
 */
struct ICypressRegistrar
    : public TRefCounted
{
    //! Initialize periodic calls of |UpdateNodes|. Attributes returned by
    //! |attributeFactory| will be set to the root node.
    virtual void Start(TCallback<NYTree::IAttributeDictionaryPtr()> attributeFactory = {}) = 0;

    //! Creates the root node and all required children. May throw.
    //!
    //! Should not be called when registrar has already been started.
    virtual TFuture<void> CreateNodes() = 0;

    //! Updates all necessary expiration times and sets |attributes| to the root node.
    //! On first iteration will also call |CreateNodes| unless |EnableImplicitInitialization|
    //! is |false|. May throw.
    //!
    //! Should not be called when registrar has already been started.
    virtual TFuture<void> UpdateNodes(NYTree::IAttributeDictionaryPtr attributes = nullptr) = 0;
};

DEFINE_REFCOUNTED_TYPE(ICypressRegistrar)

////////////////////////////////////////////////////////////////////////////////

ICypressRegistrarPtr CreateCypressRegistrar(
    TCypressRegistrarOptions&& options,
    TCypressRegistrarConfigPtr config,
    NApi::IClientPtr client,
    IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
