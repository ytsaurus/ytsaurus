package tech.ytsaurus.flow.examples.waitclickjoin.model

import javax.persistence.Column
import javax.persistence.Entity

// [BEGIN joined_action_model]
// Important! The field order must match the "joined_action" stream definition in the static spec.
@Entity
class JoinedAction {
    @Column(name = "hit_id")
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

    @Column(name = "hit_payload")
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
