def get_node_host_cell_ids(yt_client):
    node_host_cell_ids = []
    cell_descriptors = yt_client.get("//sys/@config/multicell_manager/cell_descriptors")

    try:
        items = cell_descriptors.iteritems()
    except AttributeError:
        items = cell_descriptors.items()
    for cell_id, descriptor in items:
        cell_roles = descriptor.get("roles", [])
        if "cypress_node_host" in cell_roles or "sequoia_node_host" in cell_roles:
            node_host_cell_ids.append(cell_id)

    return node_host_cell_ids
