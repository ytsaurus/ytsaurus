package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nullable;

import tech.ytsaurus.core.GUID;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

@NonNullFields
@NonNullApi
public class UpdateOperationParameters extends tech.ytsaurus.client.request.UpdateOperationParameters.BuilderBase<
        UpdateOperationParameters> {
    public UpdateOperationParameters(GUID guid) {
        setOperationId(guid);
    }

    UpdateOperationParameters(String alias) {
        setOperationAlias(alias);
    }

    public static UpdateOperationParameters fromAlias(String alias) {
        return new UpdateOperationParameters(alias);
    }

    @Override
    protected UpdateOperationParameters self() {
        return this;
    }

    public static class SchedulingOptions
            extends tech.ytsaurus.client.request.UpdateOperationParameters.SchedulingOptions {
        public SchedulingOptions() {
        }

        public SchedulingOptions(@Nullable Double weight, @Nullable ResourceLimits resourceLimits) {
            super(weight, resourceLimits);
        }
    }

    public static class ResourceLimits extends tech.ytsaurus.client.request.UpdateOperationParameters.ResourceLimits {
        public ResourceLimits() {
        }

        public ResourceLimits(@Nullable Long userSlots, @Nullable Double cpu, @Nullable Long network,
                              @Nullable Long memory) {
            super(userSlots, cpu, network, memory);
        }
    }
}
