package tech.ytsaurus.flow.examples.urldownloader.model

import javax.persistence.Entity

// Field order must match the "processed_urls" stream definition in the static spec.
@Entity
class ProcessedUrl {
    var host: String? = null

    var url: String? = null

    var data: String? = null

    constructor()

    constructor(host: String?, url: String?, data: String?) {
        this.host = host
        this.url = url
        this.data = data
    }
}
