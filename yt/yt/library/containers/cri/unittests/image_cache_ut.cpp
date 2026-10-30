#include <yt/yt/library/containers/cri/image_cache.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NContainers::NCri {
namespace {

using ::testing::_;
using ::testing::Return;
using ::testing::StrictMock;

////////////////////////////////////////////////////////////////////////////////

class TMockCriExecutor
    : public ICriExecutor
{
public:
    MOCK_METHOD(TFuture<TCriRuntimeApi::TRspStatusPtr>, GetRuntimeStatus, (bool), (override));
    MOCK_METHOD(TString, GetPodCgroup, (TString), (const, override));
    MOCK_METHOD(TFuture<TCriRuntimeApi::TRspListPodSandboxPtr>, ListPodSandbox, (
        std::function<void(NProto::PodSandboxFilter&)>), (override));
    MOCK_METHOD(TFuture<TCriRuntimeApi::TRspListContainersPtr>, ListContainers, (
        std::function<void(NProto::ContainerFilter&)>), (override));
    MOCK_METHOD(TFuture<void>, ForEachPodSandbox, (
        const TCallback<void(TCriPodDescriptorPtr, const NProto::PodSandbox&)>&,
        std::function<void(NProto::PodSandboxFilter&)>), (override));
    MOCK_METHOD(TFuture<void>, ForEachContainer, (
        const TCallback<void(const TCriDescriptor&, const NProto::Container&)>&,
        std::function<void(NProto::ContainerFilter&)>), (override));
    MOCK_METHOD(TFuture<TCriRuntimeApi::TRspPodSandboxStatusPtr>, GetPodSandboxStatus, (
        const TCriPodDescriptorPtr&, bool), (override));
    MOCK_METHOD(TFuture<TCriRuntimeApi::TRspContainerStatusPtr>, GetContainerStatus, (
        const TCriDescriptor&, bool), (override));
    MOCK_METHOD(TFuture<TCriPodDescriptorPtr>, RunPodSandbox, (TCriPodSpecPtr), (override));
    MOCK_METHOD(TFuture<void>, StopPodSandbox, (const TCriPodDescriptorPtr&), (override));
    MOCK_METHOD(TFuture<void>, RemovePodSandbox, (const TCriPodDescriptorPtr&), (override));
    MOCK_METHOD(TFuture<void>, UpdatePodResources, (
        const TCriPodDescriptorPtr&, const TCriContainerResources&), (override));
    MOCK_METHOD(void, CleanNamespace, (), (override));
    MOCK_METHOD(void, CleanPodSandbox, (const TCriPodDescriptorPtr&), (override));
    MOCK_METHOD(TFuture<TCriDescriptor>, CreateContainer, (
        TCriContainerSpecPtr, const TCriPodDescriptorPtr&, TCriPodSpecPtr), (override));
    MOCK_METHOD(TFuture<void>, StartContainer, (const TCriDescriptor&), (override));
    MOCK_METHOD(TFuture<void>, StopContainer, (const TCriDescriptor&, TDuration), (override));
    MOCK_METHOD(TFuture<void>, RemoveContainer, (const TCriDescriptor&), (override));
    MOCK_METHOD(TFuture<void>, UpdateContainerResources, (
        const TCriDescriptor&, const TCriContainerResources&), (override));
    MOCK_METHOD(TFuture<TCriImageApi::TRspListImagesPtr>, ListImages, (
        std::function<void(NProto::ImageFilter&)>), (override));
    MOCK_METHOD(TFuture<TCriImageApi::TRspImageStatusPtr>, GetImageStatus, (
        const TCriImageDescriptor&, bool), (override));
    MOCK_METHOD(TFuture<TCriImageApi::TRspPullImagePtr>, PullImage, (
        const TCriImageDescriptor&, TCriAuthConfigPtr, TCriPodSpecPtr), (override));
    MOCK_METHOD(TFuture<void>, RemoveImage, (const TCriImageDescriptor&), (override));
    MOCK_METHOD(TProcessBasePtr, CreateProcess, (
        const TString&, TCriContainerSpecPtr, TCriPodDescriptorPtr, TCriPodSpecPtr), (override));
};

////////////////////////////////////////////////////////////////////////////////

class TCriImageCacheTest
    : public ::testing::Test
{
protected:
    void CheckPullCancellation(bool cancelInitiator)
    {
        auto executor = New<StrictMock<TMockCriExecutor>>();
        auto cache = CreateCriImageCache(New<TCriImageCacheConfig>(), executor);
        auto statusPromise = NewPromise<TCriImageApi::TRspImageStatusPtr>();
        statusPromise.OnCanceled(BIND([statusPromise] (const TError& error) {
            statusPromise.TrySet(error);
        }));
        EXPECT_CALL(*executor, GetImageStatus(_, false))
            .WillOnce(Return(statusPromise.ToFuture()));

        TCriImageDescriptor image{.Image = "registry.invalid/image:test"};
        auto initiator = cache->PullImage(image);
        auto waiter = cache->PullImage(image);
        auto canceled = cancelInitiator ? initiator : waiter;
        auto remaining = cancelInitiator ? waiter : initiator;

        EXPECT_FALSE(initiator.IsSet());
        EXPECT_FALSE(waiter.IsSet());
        canceled.Cancel(TError("Job preparation canceled"));

        EXPECT_TRUE(canceled.IsSet());
        EXPECT_FALSE(remaining.IsSet());
        ASSERT_FALSE(statusPromise.IsSet());

        auto response = New<TCriImageApi::TRspImageStatus>(nullptr);
        response->mutable_image()->set_id("sha256:test");
        response->mutable_image()->set_size(1);
        statusPromise.Set(response);

        ASSERT_TRUE(canceled.IsSet());
        EXPECT_TRUE(canceled.Get().FindMatching(NYT::EErrorCode::Canceled));
        ASSERT_TRUE(remaining.IsSet());
        ASSERT_TRUE(remaining.Get().IsOK());
        EXPECT_EQ(remaining.Get().Value()->Image().Id, "sha256:test");

        auto cached = cache->LookupImage(image);
        ASSERT_TRUE(cached.IsSet());
        ASSERT_TRUE(cached.Get().IsOK());
        EXPECT_EQ(cached.Get().Value()->Image().Id, "sha256:test");
    }
};

TEST_F(TCriImageCacheTest, CancelSharedImagePull)
{
    CheckPullCancellation(/*cancelInitiator*/ false);
}

TEST_F(TCriImageCacheTest, CancelImagePullInitiator)
{
    CheckPullCancellation(/*cancelInitiator*/ true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NContainers::NCri
