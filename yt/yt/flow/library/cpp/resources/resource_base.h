#pragma once

#include "public.h"
#include "resource_controller_base.h"

#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/misc/reconfigurable.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TMaterializedFileProvider);

//! One exact file-provider revision pinned in local file storage.
class TMaterializedFileProvider
    : public TRefCounted
{
public:
    TMaterializedFileProvider(
        TFileProviderRevisionPtr revision,
        NFileStorage::IFileStorageObjectPtr storageObject);

    const TFileProviderRevisionPtr& GetRevision() const;
    const std::string& GetRootPath() const;

private:
    const TFileProviderRevisionPtr Revision_;
    const NFileStorage::IFileStorageObjectPtr StorageObject_;
    const std::string RootPath_;
};

DEFINE_REFCOUNTED_TYPE(TMaterializedFileProvider);

DECLARE_REFCOUNTED_CLASS(TMaterializedFileProviderSnapshot);

//! An immutable file snapshot with its named materialized providers.
class TMaterializedFileProviderSnapshot
    : public TRefCounted
{
public:
    TMaterializedFileProviderSnapshot(
        TFileSnapshotPtr fileSnapshot,
        THashMap<TFileProviderId, TMaterializedFileProviderPtr> fileProviders);

    const TFileSnapshotPtr& GetFileSnapshot() const;
    const THashMap<TFileProviderId, TMaterializedFileProviderPtr>& GetFileProviders() const;
    const TMaterializedFileProviderPtr& GetFileProvider(const TFileProviderId& id) const;
    const TMaterializedFileProviderPtr& GetOnlyFileProvider() const;

private:
    const TFileSnapshotPtr FileSnapshot_;
    const THashMap<TFileProviderId, TMaterializedFileProviderPtr> FileProviders_;
};

DEFINE_REFCOUNTED_TYPE(TMaterializedFileProviderSnapshot);

////////////////////////////////////////////////////////////////////////////////

//! Base class for Flow resources.
//! @see IResource for details.
class TResourceBase
    : public IResource
    , public virtual TReconfigurable<TDynamicResourceContext>
{
public:
    using TController = TResourceControllerBase;

    //! Constructor.
    /*!
     *  Resource constructor called at each Controller and Worker instances but loaded only when it is required
     *  by `required_resource_ids` section of computation static spec.
     *  So constructor has to be lightweight and non-blocking.
     *
     *  Important! Do not use constructor for resource loading and blocking calls (disk, network, etc.),
     *  use the `Load` method for that purpose instead.
     *
     *  \param context - The resource context.
     *  \param spec - The resource spec defining resource class name, parameters and dependencies.
     */
    TResourceBase(TResourceContextPtr context, TDynamicResourceContextPtr dynamicContext);

    //! Gets the resource context.
    /*!
     *  \returns The resource context.
     */
    TResourceContextPtr GetContext() const;
    TDynamicResourceContextPtr GetDynamicContext() const;

    //! Gets the resource spec.
    /*!
     *  \returns The resource spec.
     */
    TResourceSpecPtr GetSpec() const;

    //! Gets the current dynamic spec.
    /*!
     *  \returns The current dynamic resource spec.
     */
    TDynamicResourceSpecPtr GetDynamicSpec() const;

    //! Loads the resource with its dependencies.
    //! @see IResource::Load for details.
    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override;

    //! Reconfigures the resource.
    //! @see IResource::Reconfigure for details.
    void Reconfigure(const TDynamicResourceContextPtr& dynamicContext) final;

    //! Reports both ids equal to the last delivered target revision, i.e. treats switching as
    //! instant.
    //! @see IResource::GetRevisionState for the contract.
    TResourceRevisionState GetRevisionState() const override;

protected:
    //! Materializes one exact named file revision from a delivered file snapshot.
    TFuture<TMaterializedFileProviderPtr> MaterializeFileProvider(
        const TFileSnapshotPtr& fileSnapshot,
        const TFileProviderId& id) const;

    //! Materializes the requested named revisions as one immutable input snapshot.
    //! An empty |names| list means every provider declared by the resource spec.
    TFuture<TMaterializedFileProviderSnapshotPtr> MaterializeFileProviders(
        const TFileSnapshotPtr& fileSnapshot,
        const std::vector<TFileProviderId>& ids = {}) const;

    //! Reports queue activity for this resource to the resource manager.
    /*!
     *  \param morePushedToQueue - Number of additional items pushed to the queue since the last call.
     *  \param moreFetchedFromQueue - Number of additional items fetched from the queue since the last call.
     */
    void FeedStatus(i64 morePushedToQueue, i64 moreFetchedFromQueue);

    //! Gets the base parameters for resource.
    /*!
     *  This method shouldn't be called directly.
     *  Use YT_FLOW_EXTEND_PARAMETERS macro for registering your own parameters and GetParameters() method to access them.
     *
     *  \returns The YSON structure containing the resource parameters.
     */
    NYTree::TYsonStructPtr GetParametersBase() const final;

    //! Gets the base dynamic parameters for resource.
    /*!
     *  This method shouldn't be called directly.
     *  Use YT_FLOW_EXTEND_DYNAMIC_PARAMETERS macro for registering your own dynamic parameters
     *  and GetDynamicParameters() method to access them.
     *
     *  \returns The YSON structure containing the resource dynamic parameters.
     */
    NYTree::TYsonStructPtr GetDynamicParametersBase() const final;

private:
    const TResourceContextPtr Context_;
    TAtomicIntrusivePtr<TDynamicResourceContext> DynamicContext_;
    const NYTree::TYsonStructPtr Parameters_;
    TAtomicIntrusivePtr<NYTree::TYsonStruct> DynamicParameters_;
    THashMap<TFileProviderId, IFileProviderPtr> FileProviders_;

protected:
    NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
