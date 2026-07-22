package tech.ytsaurus.flow.examples.urldownloader.model

import javax.persistence.Entity

// Field order must match the "urls" stream definition in the static spec.
@Entity
class UrlMessage {
    var host: String? = null

    var url: String? = null

    constructor()

    constructor(host: String?, url: String?) {
        this.host = host
        this.url = url
    }
}
