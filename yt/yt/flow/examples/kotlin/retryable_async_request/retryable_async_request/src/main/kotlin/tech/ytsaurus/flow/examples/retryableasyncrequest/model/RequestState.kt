package tech.ytsaurus.flow.examples.retryableasyncrequest.model

import javax.persistence.Column
import javax.persistence.Entity

// [BEGIN request_state]
@Entity
class RequestState {
    @Column(name = "request_id")
    var requestId: Long = 0

    var key: Long = 0

    var request: String? = null

    @Column(name = "failed_attempts")
    var failedAttempts: Int = 0

    constructor()

    constructor(requestId: Long, key: Long, request: String?, failedAttempts: Int) {
        this.requestId = requestId
        this.key = key
        this.request = request
        this.failedAttempts = failedAttempts
    }

    override fun toString(): String {
        return "RequestState{" +
            "requestId=" + requestId +
            ", key=" + key +
            ", request='" + request + '\'' +
            ", failedAttempts=" + failedAttempts +
            '}'
    }
}
// [END request_state]
