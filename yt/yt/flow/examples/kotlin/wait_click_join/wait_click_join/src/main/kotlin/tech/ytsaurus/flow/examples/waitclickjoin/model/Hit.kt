package tech.ytsaurus.flow.examples.waitclickjoin.model

import javax.persistence.Column
import javax.persistence.Entity

// [BEGIN hit_model]
// The stream schema is derived from this class: the runner writes it into the pipeline spec,
// so "hit" needs no schema of its own there. Field order defines column order, and
// columnDefinition pins a column type where the default for the field type differs.
@Entity
class Hit {
    @Column(name = "hit_id", columnDefinition = "string")
    var hitId: String? = null

    @Column(name = "hit_time", columnDefinition = "uint64")
    var hitTime: Long? = null

    @Column(name = "hit_payload", columnDefinition = "string")
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
