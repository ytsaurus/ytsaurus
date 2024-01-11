from .helpers import mount_table, unmount_table
from .logger import logger

import yt.wrapper as yt

import random

def reshard_table(table, schema, tablet_count, new_tablet_count=None, ordered=False):
    if new_tablet_count is None:
        new_tablet_count = max(1, tablet_count + random.randint(-3, 3))
    logger.info("Resharding table %s into %s tablets", table, new_tablet_count)
    unmount_table(table)
    if ordered:
        yt.reshard_table(table, tablet_count=new_tablet_count, sync=True)
    else:
        yt.reshard_table(table, schema.create_pivot_keys(new_tablet_count), sync=True)
    mount_table(table)

def reshard_table_partly(table, schema, first_tablet_index, last_tablet_index):
    new_tablet_count = max(1, last_tablet_index - first_tablet_index + 1 + random.randint(-3, 3))
    logger.info(
        "Resharding table %s, tablets %s..%s into %s tablets",
        table, first_tablet_index, last_tablet_index, new_tablet_count)
    try:
        new_pivots = schema.create_pivot_keys_for_tablet_range(
            new_tablet_count, first_tablet_index, last_tablet_index)
    except:
        logger.warning("Failed to generate new pivots", exc_info=True)
        return

    unmount_table(table)
    yt.reshard_table(table, new_pivots, first_tablet_index=first_tablet_index, last_tablet_index=last_tablet_index, sync=True)
    mount_table(table)

def reshard_multiple_times(table, schema):
    def num_tablets():
        return max(1, random.randint(tablet_count - 5, tablet_count + 5))
    for i in range(2):
        tablet_count = yt.get("{}/@tablet_count".format(table))
        first = random.randint(0, tablet_count - 1)
        last = random.randint(0, tablet_count - 1)
        if first > last:
            first, last = last, first
        reshard_table_partly(table, schema, first, last)
    for i in range(2):
        tablet_count = yt.get("{}/@tablet_count".format(table))
        reshard_table(table, schema, num_tablets())
