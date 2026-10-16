package tech.ytsaurus.flow.examples.urldownloader.model

import javax.persistence.Column
import javax.persistence.Entity

// [BEGIN host_state]
@Entity
class HostState {
    var host: String? = null

    @Column(name = "pending_urls")
    var pendingUrls: MutableList<String?>? = ArrayList()

    constructor()

    constructor(host: String?) {
        this.host = host
    }
}
// [END host_state]
