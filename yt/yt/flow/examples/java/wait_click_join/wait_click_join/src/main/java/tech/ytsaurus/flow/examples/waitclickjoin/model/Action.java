package tech.ytsaurus.flow.examples.waitclickjoin.model;

import javax.persistence.Column;
import javax.persistence.Entity;

// [BEGIN action_model]
// Important! The field order must match the "action" stream definition in the static spec.
@Entity
public class Action {
    @Column(name = "hit_id")
    private String hitId;
    @Column(name = "hit_time", columnDefinition = "uint64")
    private Long hitTime;
    @Column(name = "is_click")
    private Boolean click;
    @Column(name = "action_time", columnDefinition = "uint64")
    private Long actionTime;

    public Action() {
    }

    public Action(String hitId, Long hitTime, Long actionTime, Boolean isClick) {
        this.hitId = hitId;
        this.hitTime = hitTime;
        this.actionTime = actionTime;
        this.click = isClick;
    }

    public String getHitId() {
        return hitId;
    }

    public void setHitId(String hitId) {
        this.hitId = hitId;
    }

    public Long getHitTime() {
        return hitTime;
    }

    public void setHitTime(Long hitTime) {
        this.hitTime = hitTime;
    }

    public Long getActionTime() {
        return actionTime;
    }

    public void setActionTime(Long actionTime) {
        this.actionTime = actionTime;
    }

    public Boolean isClick() {
        return click;
    }

    public void setIsClick(Boolean click) {
        this.click = click;
    }

    @Override
    public String toString() {
        return "Action{" +
                "hitId='" + hitId + '\'' +
                ", hitTime=" + hitTime +
                ", actionTime=" + actionTime +
                ", click=" + click +
                '}';
    }
}
// [END action_model]
