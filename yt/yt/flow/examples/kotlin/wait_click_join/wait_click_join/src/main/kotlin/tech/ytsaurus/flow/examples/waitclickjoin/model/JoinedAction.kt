package tech.ytsaurus.flow.examples.waitclickjoin.model

import javax.persistence.Column
import javax.persistence.Entity

// [BEGIN joined_action_model]
// The stream schema is derived from this class: the runner writes it into the pipeline spec,
// so "joined_action" needs no schema of its own there. Field order defines column order, and
// columnDefinition pins a column type where the default for the field type differs.
@Entity
class JoinedAction {
    @Column(name = "hit_id", columnDefinition = "string")
    var hitId: String? = null

    @Column(name = "hit_time", columnDefinition = "uint64")
    var hitTime: Long? = null

    @get:JvmName("getClick")
    @set:JvmName("setClick")
    @Column(name = "is_click")
    var isClick: Boolean? = null

    @Column(name = "show_time", columnDefinition = "uint64")
    var showTime: Long? = null

    @Column(name = "click_time", columnDefinition = "uint64")
    var clickTime: Long? = null

    @Column(name = "hit_payload", columnDefinition = "string")
    var hitPayload: String? = null

    constructor()

    constructor(hitId: String, hitTime: Long, isClick: Boolean, showTime: Long, clickTime: Long, hitPayload: String) {
        this.hitId = hitId
        this.hitTime = hitTime
        this.isClick = isClick
        this.showTime = showTime
        this.clickTime = clickTime
        this.hitPayload = hitPayload
    }
}
// [END joined_action_model]
