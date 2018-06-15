#!/usr/bin/python

import yt.wrapper as yt
from yt.wrapper.native_driver import get_driver_instance

virtual_multicell_map_nodes = [
    ["tablet_action_map", "//sys/tablet_actions"],
    ["tablet_map", "//sys/tablets"]
]

def get_config():
    driver = get_driver_instance(None)
    return driver.get_config()

def create_virtual_multicell_map_node(virtual_multicell_map_node):
    def create_at_cell(master_cell_id):
        yt.config.COMMAND_PARAMS["master_cell_id"] = master_cell_id
        if not yt.exists(virtual_multicell_map_node[1]):
            yt.create(*virtual_multicell_map_node)
        yt.config.COMMAND_PARAMS.pop("master_cell_id")

    config = get_config()
    primary_cell_id = config["primary_master"]["cell_id"]
    secondary_cell_ids = [cell["cell_id"] for cell in config["secondary_masters"]]
    cell_ids = [primary_cell_id] + secondary_cell_ids

    for master_cell_id in cell_ids:
        create_at_cell(master_cell_id)

if __name__ == "__main__":
    for virtual_multicell_map_node in virtual_multicell_map_nodes:
        create_virtual_multicell_map_node(virtual_multicell_map_node)

