package ru.yandex.yt.ytclient.rpc;

import ru.yandex.yt.rpc.TReqDiscover;
import ru.yandex.yt.rpc.TRspDiscover;

/**
 * Базовый интерфейс для rpc сервисов в yt
 */
public interface DiscoverableRpcService {
    RpcClientRequestBuilder<TReqDiscover.Builder, RpcClientResponse<TRspDiscover>> discover();
}
