package ru.yandex.yt.ytclient.proxy.request;

import javax.annotation.Nullable;

import ru.yandex.inside.yt.kosher.common.GUID;
import ru.yandex.lang.NonNullApi;
import ru.yandex.lang.NonNullFields;

@NonNullFields
@NonNullApi
public class UpdateOperationParameters extends ru.yandex.yt.ytclient.request.UpdateOperationParameters.BuilderBase<
        UpdateOperationParameters, ru.yandex.yt.ytclient.request.UpdateOperationParameters> {
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

    @Override
    public ru.yandex.yt.ytclient.request.UpdateOperationParameters build() {
        return new ru.yandex.yt.ytclient.request.UpdateOperationParameters(this);
    }

    public static class SchedulingOptions
            extends ru.yandex.yt.ytclient.request.UpdateOperationParameters.SchedulingOptions {
        public SchedulingOptions() {
        }

        public SchedulingOptions(@Nullable Double weight, @Nullable ResourceLimits resourceLimits) {
            super(weight, resourceLimits);
        }
    }

    public static class ResourceLimits extends ru.yandex.yt.ytclient.request.UpdateOperationParameters.ResourceLimits {
        public ResourceLimits() {
        }

        public ResourceLimits(@Nullable Long userSlots, @Nullable Double cpu, @Nullable Long network,
                              @Nullable Long memory) {
            super(userSlots, cpu, network, memory);
        }
    }
}
