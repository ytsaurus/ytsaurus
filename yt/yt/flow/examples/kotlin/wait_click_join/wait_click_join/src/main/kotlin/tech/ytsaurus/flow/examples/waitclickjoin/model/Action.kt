package tech.ytsaurus.flow.examples.waitclickjoin.model

import javax.persistence.Column
import javax.persistence.Entity

// [BEGIN action_model]
// The stream schema is derived from this class: the runner writes it into the pipeline spec,
// so "action" needs no schema of its own there. Field order defines column order, and
// columnDefinition pins a column type where the default for the field type differs.
@Entity
class Action {
    @Column(name = "hit_id", columnDefinition = "string")
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
