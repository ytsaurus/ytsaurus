import aiohttp
import argparse
import asyncio
import datetime
import logging
import time
import typing
import yt.wrapper as yt

STUCK_STATES = {
    "persistent_commit_prepared",
}

TABLET_CELLS_PATH = "//sys/tablet_cells"

MONITORING_PORT = "10012"

TIMESTAMP_COUNTER_WIDTH = 30

RETRY_COUNT = 3
RETRY_BACKOFF = 5

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"

TIMEZONE = datetime.timezone(datetime.timedelta(hours=3))

logger = logging.getLogger("collect_stuck_transactions")


class Cell(typing.NamedTuple):
    id: str
    node_address: str

    @property
    def transactions_url(self):
        node_address_without_port = self.node_address.split(":")[0]
        return (
            f"http://{node_address_without_port}:{MONITORING_PORT}"
            f"/orchid/tablet_cells/{self.id}/transactions"
        )


def timestamp_to_datetime(timestamp):
    return datetime.datetime.fromtimestamp(timestamp >> TIMESTAMP_COUNTER_WIDTH, tz=TIMEZONE)


def parse_time(value):
    result = datetime.datetime.fromisoformat(value)
    if result.tzinfo is None:
        result = result.replace(tzinfo=TIMEZONE)
    return result


def parse_positive_int(value):
    result = int(value)
    if result <= 0:
        raise argparse.ArgumentTypeError(f"must be positive, got {value}")
    return result


def setup_logging():
    formatter = logging.Formatter(LOG_FORMAT)
    formatter.converter = lambda timestamp: datetime.datetime.fromtimestamp(timestamp, tz=TIMEZONE).timetuple()

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    logging.basicConfig(level=logging.INFO, handlers=[handler])


def find_leader_address(peers):
    for peer in peers:
        if peer.get("state") == "leading" and not peer.get("alien", False) and peer.get("address"):
            return peer["address"]
    return None


def process_cell(cell_id, peers, cells, cells_without_leader):
    leader_address = find_leader_address(peers)
    if leader_address is None:
        cells_without_leader[cell_id] = f"No leading peer: {peers}"
    else:
        cells.append(Cell(id=cell_id, node_address=leader_address))


def get_leading_peers(cell_filter):
    cells = yt.list(TABLET_CELLS_PATH, attributes=["peers", "tablet_cell_bundle"])
    logger.info(f"Found {len(cells)} tablet cells")

    if cell_filter is not None:
        cells = [cell for cell in cells if cell_filter(cell)]
        logger.info(f"Found {len(cells)} tablet cells after filtering by bundle")

    active_cells = []
    cells_without_leader = {}
    for cell in cells:
        process_cell(str(cell), cell.attributes.get("peers", []), active_cells, cells_without_leader)

    logger.info(
        f"Found leading peers for {len(active_cells)} tablet cells, "
        f"{len(cells_without_leader)} tablet cells have no leading peer"
    )

    for attempt in range(1, RETRY_COUNT + 1):
        if not cells_without_leader:
            break

        logger.info(
            f"Retrying to find leading peers of {len(cells_without_leader)} tablet cells "
            f"in {RETRY_BACKOFF} seconds (attempt {attempt}/{RETRY_COUNT})"
        )
        time.sleep(RETRY_BACKOFF)

        still_without_leader = {}
        for cell_id in cells_without_leader:
            try:
                peers = yt.get(f"{TABLET_CELLS_PATH}/{cell_id}/@peers")
            except Exception as ex:
                still_without_leader[cell_id] = f"Failed to get peers: {ex}"
            else:
                process_cell(cell_id, peers, active_cells, still_without_leader)

            if cell_id in still_without_leader:
                logger.info(f"Tablet cell {cell_id}: {still_without_leader[cell_id]}")
        cells_without_leader = still_without_leader

    return active_cells, cells_without_leader


async def fetch_url(session, semaphore, url):
    async with semaphore:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json(content_type=None)


async def fetch_urls(concurrency, urls):
    semaphore = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=10,
        force_close=True,
    )
    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        return await asyncio.gather(*[fetch_url(session, semaphore, url) for url in urls], return_exceptions=True)


def fetch_transactions(concurrency, cells):
    logger.info(f"Fetching transactions from {len(cells)} leading peers (Concurrency: {concurrency})")

    results = {}
    errors = {}
    pending = cells
    for attempt in range(1, RETRY_COUNT + 1):
        responses = asyncio.run(fetch_urls(concurrency, [cell.transactions_url for cell in pending]))

        errors = {}
        for cell, response in zip(pending, responses):
            if isinstance(response, Exception):
                errors[cell] = f"{type(response).__name__}: {response}"
                logger.info(f"Request {cell.transactions_url} failed: {errors[cell]}")
            else:
                results[cell] = response

        if not errors:
            break

        pending = list(errors)
        logger.warning(f"{len(pending)} requests failed at attempt {attempt}/{RETRY_COUNT}")
        if attempt < RETRY_COUNT:
            time.sleep(RETRY_BACKOFF)

    logger.info(f"Fetched transactions from {len(results)} tablet cells, {len(errors)} failed")
    return results, errors


def filter_transactions(results, created_before):
    filtered = []
    for cell, transactions in results.items():
        for tx_id, tx_info in transactions.items():
            if "@" in tx_id:
                logger.info(
                    f"Skipping externalized transaction (TransactionId: {tx_id}, TabletCellId: {cell.id}, "
                    f"NodeAddress: {cell.node_address}, Info: {tx_info})"
                )
                continue

            if tx_info["state"] not in STUCK_STATES:
                continue

            if timestamp_to_datetime(tx_info["start_timestamp"]) >= created_before:
                continue

            filtered.append((cell, tx_id, tx_info))
    return sorted(filtered, key=lambda entry: entry[:2])


def log_transactions(transactions):
    logger.info(f"Selected {len(transactions)} stuck transactions")
    for cell, tx_id, tx_info in transactions:
        logger.info(
            f"Stuck transaction (TransactionId: {tx_id}, TabletCellId: {cell.id}, NodeAddress: {cell.node_address}, "
            f"State: {tx_info['state']}, StartTimestamp: {tx_info['start_timestamp']}, "
            f"StartTime: {timestamp_to_datetime(tx_info['start_timestamp']).isoformat()}, Info: {tx_info})"
        )


def log_failures(cells_without_leader, errors):
    if cells_without_leader:
        logger.warning(
            f"Tablet cells without leading peer after {RETRY_COUNT} retries ({len(cells_without_leader)}): "
            f"{' '.join(sorted(cells_without_leader))}"
        )

        for cell_id, error in sorted(cells_without_leader.items()):
            logger.warning(f"Tablet cell without leading peer (TabletCellId: {cell_id}): {error}")

    if errors:
        failed_nodes = sorted({cell.node_address for cell in errors})
        failed_cells = sorted({cell.id for cell in errors})
        logger.warning(f"Failed to fetch transactions after {RETRY_COUNT} attempts for {len(errors)} tablet cells")
        logger.warning(f"Failed node addresses ({len(failed_nodes)}): {' '.join(failed_nodes)}")
        logger.warning(f"Failed tablet cell ids ({len(failed_cells)}): {' '.join(failed_cells)}")

        for cell, error in sorted(errors.items()):
            logger.warning(f"Failed request (NodeAddress: {cell.node_address}, TabletCellId: {cell.id}): {error}")


def make_cell_filter(args):
    if args.bundle is not None:
        return lambda cell: cell.attributes.get("tablet_cell_bundle") == args.bundle
    if args.bundle_prefix is not None:
        return lambda cell: cell.attributes.get("tablet_cell_bundle", "").startswith(args.bundle_prefix)
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Collect stuck tablet transactions from leading peers of all tablet cells via orchid "
        "and write \"<tablet_cell_id> <tx_id>\" pairs to the output file"
    )
    parser.add_argument("--proxy", type=yt.config.set_proxy, required=True, help="yt proxy")
    parser.add_argument(
        "--concurrency",
        type=parse_positive_int,
        default=100,
        help="Max number of parallel orchid requests",
    )
    parser.add_argument(
        "--created-before",
        type=parse_time,
        required=True,
        help="Only transactions started before this time; ISO format (UTC+3 if no tz)",
    )
    parser.add_argument("--output", default="stuck_transactions", help="Output file")
    bundle_group = parser.add_mutually_exclusive_group()
    bundle_group.add_argument("--bundle", help="Only tablet cells of this tablet cell bundle")
    bundle_group.add_argument("--bundle-prefix", help="Only tablet cells of tablet cell bundles with this prefix")
    args = parser.parse_args()

    setup_logging()

    logger.info(
        f"Collecting transactions (Proxy: {yt.config['proxy']['url']}, Concurrency: {args.concurrency}, "
        f"CreatedBefore: {args.created_before.isoformat()}, States: {', '.join(sorted(STUCK_STATES))}, "
        f"Bundle: {args.bundle}, BundlePrefix: {args.bundle_prefix})"
    )

    cells, cells_without_leader = get_leading_peers(make_cell_filter(args))
    results, errors = fetch_transactions(args.concurrency, cells)

    transactions = filter_transactions(results, args.created_before)

    with open(args.output, "w") as output:
        for cell, tx_id, _ in transactions:
            output.write(f"{cell.id} {tx_id}\n")
    logger.info(f"Written {len(transactions)} transactions to {args.output}")

    log_transactions(transactions)
    log_failures(cells_without_leader, errors)


if __name__ == "__main__":
    main()
