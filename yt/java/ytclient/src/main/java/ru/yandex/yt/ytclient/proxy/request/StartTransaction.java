package ru.yandex.yt.ytclient.proxy.request;

import tech.ytsaurus.client.request.TransactionType;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Request for starting transaction.
 *
 * @see <a href="https://docs.yandex-team.ru/yt/api/commands#start_tx">
 *     start_tx documentation
 *     </a>
 * @see <a href="https://docs.yandex-team.ru/yt/description/dynamic_tables/sorted_dynamic_tables">
 *     dynamic tables documentation
 *     </a>
 */
@NonNullApi
@NonNullFields
public class StartTransaction extends tech.ytsaurus.client.request.StartTransaction.BuilderBase<StartTransaction> {
    public StartTransaction(TransactionType type) {
        this(type, type == TransactionType.Tablet);
    }

    private StartTransaction(TransactionType type, boolean sticky) {
        setType(type).setSticky(sticky);
    }

    /**
     * Create request for starting master transaction.
     *
     * Master transactions are for working with static tables and cypress objects.
     */
    public static StartTransaction master() {
        return new StartTransaction(TransactionType.Master);
    }

    /**
     * Create request for starting tablet transaction.
     *
     * Tablet transactions are for working with dynamic tables.
     */
    public static StartTransaction tablet() {
        return new StartTransaction(TransactionType.Tablet);
    }

    /**
     * Create request for starting sticky master transaction.
     *
     * Such type of transactions can be used to work with all types of objects: cypress / static tables / dynamic tables.
     * Though their usage is discouraged: prefer to use either master or tablet transactions.
     * Compared to tablet transactions they create additional load on masters and have other special effects that you
     * might not want to have.
     */
    public static StartTransaction stickyMaster() {
        return new StartTransaction(TransactionType.Master, true);
    }

    @Override
    protected StartTransaction self() {
        return this;
    }
}
