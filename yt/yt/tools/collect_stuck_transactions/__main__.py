import aiohttp
import argparse
import asyncio
import datetime
import logging
import sys
import time
import typing
import yt.wrapper as yt

STUCK_STATES = {
    "persistent_commit_prepared",
}

STUCK_COMMIT_STATES = {
    "prepare",
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
    def orchid_url(self):
        node_address_without_port = self.node_address.split(":")[0]
        return f"http://{node_address_without_port}:{MONITORING_PORT}/orchid/tablet_cells/{self.id}"

    @property
    def transactions_url(self):
        return f"{self.orchid_url}/transactions"

    @property
    def persistent_commits_url(self):
        return f"{self.orchid_url}/transaction_supervisor/persistent_commits"


class Transaction(typing.NamedTuple):
    cell: Cell
    id: str

    @property
    def persistent_commit_url(self):
        return f"{self.cell.persistent_commits_url}/{self.id}"


def timestamp_to_datetime(timestamp):
    return datetime.datetime.fromtimestamp(timestamp >> TIMESTAMP_COUNTER_WIDTH, tz=TIMEZONE)


def timestamp_from_tx_id(tx_id):
    parts = tx_id.split("-")
    # Sequoia transaction ids have bit 62 of the counter set, see SequoiaCounterMask.
    return ((int(parts[0], 16) << 32) | int(parts[1], 16)) & ~0x4000000000000000


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


def highlight(text):
    if sys.stderr.isatty():
        return f"\033[1;31m{text}\033[0m"
    return text


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
            # Orchid puts YT error into the header and leaves the body empty.
            if response.status >= 400 and "X-YT-Error" in response.headers:
                raise RuntimeError(f"HTTP {response.status}: {response.headers['X-YT-Error']}")
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


def fetch_with_retries(concurrency, requests, get_url, description):
    logger.info(f"Fetching {description} with {len(requests)} subrequests (Concurrency: {concurrency})")

    results = {}
    errors = {}
    pending = requests
    for attempt in range(1, RETRY_COUNT + 1):
        responses = asyncio.run(fetch_urls(concurrency, [get_url(request) for request in pending]))

        errors = {}
        for request, response in zip(pending, responses):
            if isinstance(response, Exception):
                errors[request] = f"{type(response).__name__}: {response}"
                logger.info(f"Request {get_url(request)} failed: {errors[request]}")
            else:
                results[request] = response

        if not errors:
            break

        pending = list(errors)
        logger.warning(f"{len(pending)} subrequests failed at attempt {attempt}/{RETRY_COUNT}")
        if attempt < RETRY_COUNT:
            time.sleep(RETRY_BACKOFF)

    logger.info(f"Fetched {description}: {len(results)} subrequests succeeded, {len(errors)} failed")
    return results, errors


def fetch_transactions(concurrency, cells):
    return fetch_with_retries(concurrency, cells, lambda cell: cell.transactions_url, "transactions")


def fetch_persistent_commits(concurrency, cells, created_before):
    tx_ids_by_cell, cell_errors = fetch_with_retries(
        concurrency, cells, lambda cell: cell.persistent_commits_url, "persistent commit ids",
    )

    transactions = []
    for cell, tx_ids in tx_ids_by_cell.items():
        # Virtual map has attributes only if it is incomplete (more than 1000 keys);
        # then it is returned as {"$attributes": {"incomplete": true}, "$value": {...}}.
        if "$value" in tx_ids:
            tx_ids = tx_ids["$value"]
            logger.warning(highlight(
                f"Persistent commits are incomplete, only first {len(tx_ids)} are fetched "
                f"(TabletCellId: {cell.id}, NodeAddress: {cell.node_address})"
            ))
        transactions.extend(Transaction(cell=cell, id=tx_id) for tx_id in tx_ids)

    # Prepare timestamp is never less than start timestamp, so commits started after created_before are not stuck.
    candidates = [
        transaction for transaction in transactions
        if timestamp_to_datetime(timestamp_from_tx_id(transaction.id)) < created_before
    ]
    logger.info(
        f"Found {len(transactions)} persistent commits, "
        f"{len(transactions) - len(candidates)} of them started after {created_before.isoformat()} and skipped"
    )

    results, commit_errors = fetch_with_retries(
        concurrency, candidates, lambda commit: commit.persistent_commit_url, "persistent commits",
    )
    return results, cell_errors, commit_errors


def filter_transactions(results, created_before):
    filtered = []
    for cell, transactions in results.items():
        for tx_id, info in transactions.items():
            if "@" in tx_id:
                logger.info(
                    f"Skipping externalized transaction (TransactionId: {tx_id}, TabletCellId: {cell.id}, "
                    f"NodeAddress: {cell.node_address}, Info: {info})"
                )
                continue

            if info["state"] not in STUCK_STATES:
                continue

            if timestamp_to_datetime(info["start_timestamp"]) >= created_before:
                continue

            filtered.append((Transaction(cell=cell, id=tx_id), info))
    return sorted(filtered, key=lambda entry: entry[0])


def filter_commits(results, created_before):
    filtered = []
    for commit, info in results.items():
        if info["persistent_state"] not in STUCK_COMMIT_STATES:
            continue

        if info["prepare_timestamp"] == 0:
            continue

        if timestamp_to_datetime(info["prepare_timestamp"]) >= created_before:
            continue

        filtered.append((commit, info))
    return sorted(filtered, key=lambda entry: entry[0])


def log_transactions(transactions):
    logger.info(f"Selected {len(transactions)} stuck transactions")
    for transaction, info in transactions:
        logger.info(
            f"Stuck transaction (TransactionId: {transaction.id}, TabletCellId: {transaction.cell.id}, "
            f"NodeAddress: {transaction.cell.node_address}, "
            f"State: {info['state']}, StartTimestamp: {info['start_timestamp']}, "
            f"StartTime: {timestamp_to_datetime(info['start_timestamp']).isoformat()}, Info: {info})"
        )


def log_commits(commits):
    logger.info(f"Selected {len(commits)} stuck persistent commits")
    for commit, info in commits:
        logger.info(
            f"Stuck persistent commit (TransactionId: {commit.id}, TabletCellId: {commit.cell.id}, "
            f"NodeAddress: {commit.cell.node_address}, PersistentState: {info['persistent_state']}, "
            f"PrepareTimestamp: {info['prepare_timestamp']}, "
            f"PrepareTime: {timestamp_to_datetime(info['prepare_timestamp']).isoformat()}, Info: {info})"
        )


def log_request_failures(description, errors, get_cell, get_url):
    if not errors:
        return

    failed_cells = sorted({get_cell(request) for request in errors})
    logger.warning(f"Failed to fetch {description} after {RETRY_COUNT} attempts for {len(errors)} subrequests")
    logger.warning(
        f"Failed cells ({len(failed_cells)}): {', '.join(f'{cell.id} ({cell.node_address})' for cell in failed_cells)}"
    )

    for request, error in sorted(errors.items()):
        cell = get_cell(request)
        logger.warning(
            f"Failed request {get_url(request)} (NodeAddress: {cell.node_address}, TabletCellId: {cell.id}): {error}"
        )


def log_failures(cells_without_leader, transaction_errors, cell_errors, commit_errors):
    if cells_without_leader:
        logger.warning(
            f"Tablet cells without leading peer after {RETRY_COUNT} retries ({len(cells_without_leader)}): "
            f"{' '.join(sorted(cells_without_leader))}"
        )

        for cell_id, error in sorted(cells_without_leader.items()):
            logger.warning(f"Tablet cell without leading peer (TabletCellId: {cell_id}): {error}")

    log_request_failures("transactions", transaction_errors, lambda cell: cell, lambda cell: cell.transactions_url)
    log_request_failures(
        "persistent commit ids", cell_errors, lambda cell: cell, lambda cell: cell.persistent_commits_url,
    )
    log_request_failures(
        "persistent commits", commit_errors, lambda commit: commit.cell, lambda commit: commit.persistent_commit_url,
    )


def make_cell_filter(args):
    if args.bundle is not None:
        return lambda cell: cell.attributes.get("tablet_cell_bundle") == args.bundle
    if args.bundle_prefix is not None:
        return lambda cell: cell.attributes.get("tablet_cell_bundle", "").startswith(args.bundle_prefix)
    return None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Collect stuck tablet transactions and persistent commits from leading peers of all tablet cells "
        "via orchid and write \"<tablet_cell_id> <tx_id>\" pairs to the output files"
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
        help="Only transactions started (persistent commits prepared) before this time; ISO format (UTC+3 if no tz)",
    )
    parser.add_argument("--output", default="stuck_transactions", help="Output file for stuck transactions")
    parser.add_argument(
        "--replication-output",
        default="stuck_replication_transactions",
        help="Output file for stuck persistent commits",
    )
    parser.add_argument(
        "--mode",
        choices=["transactions", "persistent-commits", "all"],
        default="all",
        help="What to collect: transactions, persistent commits or both",
    )
    bundle_group = parser.add_mutually_exclusive_group()
    bundle_group.add_argument("--bundle", help="Only tablet cells of this tablet cell bundle")
    bundle_group.add_argument("--bundle-prefix", help="Only tablet cells of tablet cell bundles with this prefix")
    return parser.parse_args()


def main():
    args = parse_args()

    setup_logging()

    logger.info(
        f"Collecting transactions (Proxy: {yt.config['proxy']['url']}, Mode: {args.mode}, "
        f"Concurrency: {args.concurrency}, CreatedBefore: {args.created_before.isoformat()}, "
        f"States: {', '.join(sorted(STUCK_STATES))}, CommitStates: {', '.join(sorted(STUCK_COMMIT_STATES))}, "
        f"Bundle: {args.bundle}, BundlePrefix: {args.bundle_prefix})"
    )

    cells, cells_without_leader = get_leading_peers(make_cell_filter(args))

    transaction_errors = {}
    if args.mode in ("transactions", "all"):
        results, transaction_errors = fetch_transactions(args.concurrency, cells)

        transactions = filter_transactions(results, args.created_before)

        with open(args.output, "w") as output:
            for transaction, _ in transactions:
                output.write(f"{transaction.cell.id} {transaction.id}\n")
        logger.info(f"Written {len(transactions)} transactions to {args.output}")

        log_transactions(transactions)

    cell_errors = {}
    commit_errors = {}
    if args.mode in ("persistent-commits", "all"):
        results, cell_errors, commit_errors = fetch_persistent_commits(args.concurrency, cells, args.created_before)

        commits = filter_commits(results, args.created_before)

        with open(args.replication_output, "w") as output:
            for commit, _ in commits:
                output.write(f"{commit.cell.id} {commit.id}\n")
        logger.info(f"Written {len(commits)} persistent commits to {args.replication_output}")

        log_commits(commits)

    log_failures(cells_without_leader, transaction_errors, cell_errors, commit_errors)


if __name__ == "__main__":
    main()
