package tech.ytsaurus.flow.examples.waitclickjoin.model

import javax.persistence.Column
import javax.persistence.Entity

// [BEGIN hit_model]
// Important! The field order must match the "hit" stream definition in the static spec.
@Entity
class Hit {
    @Column(name = "hit_id")
    var hitId: String? = null

    @Column(name = "hit_time", columnDefinition = "uint64")
    var hitTime: Long? = null

    @Column(name = "hit_payload")
    var hitPayload: String? = null

    constructor()

    constructor(hitId: String, hitTime: Long, hitPayload: String) {
        this.hitId = hitId
        this.hitTime = hitTime
        this.hitPayload = hitPayload
    }

    override fun toString(): String {
        return "Hit{" +
            "hitId='$hitId'" +
            ", hitTime=$hitTime" +
            ", hitPayload='$hitPayload'" +
            '}'
    }
}
// [END hit_model]
