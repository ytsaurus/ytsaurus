package tech.ytsaurus.flow.examples.waitclickjoin.model

import javax.persistence.Column
import javax.persistence.Entity

// [BEGIN action_model]
// Important! The field order must match the "action" stream definition in the static spec.
@Entity
class Action {
    @Column(name = "hit_id")
    var hitId: String? = null

    @Column(name = "hit_time", columnDefinition = "uint64")
    var hitTime: Long? = null

    @Column(name = "is_click")
    var click: Boolean? = null

    @Column(name = "action_time", columnDefinition = "uint64")
    var actionTime: Long? = null

    constructor()

    constructor(hitId: String, hitTime: Long, actionTime: Long, isClick: Boolean) {
        this.hitId = hitId
        this.hitTime = hitTime
        this.actionTime = actionTime
        this.click = isClick
    }

    fun isClick(): Boolean? = click

    fun setIsClick(click: Boolean?) {
        this.click = click
    }

    override fun toString(): String {
        return "Action{" +
            "hitId='$hitId'" +
            ", hitTime=$hitTime" +
            ", actionTime=$actionTime" +
            ", click=$click" +
            '}'
    }
}
// [END action_model]
