package tech.ytsaurus.flow.examples.waitclickjoin.model;

import javax.persistence.Column;
import javax.persistence.Entity;

// [BEGIN joined_action_model]
// The stream schema is derived from this class: the runner writes it into the pipeline spec,
// so "joined_action" needs no schema of its own there. Field order defines column order, and
// columnDefinition pins a column type where the default for the field type differs.
@Entity
public class JoinedAction {
    @Column(name = "hit_id", columnDefinition = "string")
    private String hitId;
    @Column(name = "hit_time", columnDefinition = "uint64")
    private Long hitTime;
    @Column(name = "is_click")
    private Boolean isClick;
    @Column(name = "show_time", columnDefinition = "uint64")
    private Long showTime;
    @Column(name = "click_time", columnDefinition = "uint64")
    private Long clickTime;
    @Column(name = "hit_payload", columnDefinition = "string")
    private String hitPayload;

    public JoinedAction() {
    }

    public JoinedAction(String hitId, Long hitTime, Boolean isClick, Long showTime, Long clickTime, String hitPayload) {
        this.hitId = hitId;
        this.hitTime = hitTime;
        this.isClick = isClick;
        this.showTime = showTime;
        this.clickTime = clickTime;
        this.hitPayload = hitPayload;
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

    public Boolean getClick() {
        return isClick;
    }

    public void setClick(Boolean click) {
        isClick = click;
    }

    public Long getShowTime() {
        return showTime;
    }

    public void setShowTime(Long showTime) {
        this.showTime = showTime;
    }

    public Long getClickTime() {
        return clickTime;
    }

    public void setClickTime(Long clickTime) {
        this.clickTime = clickTime;
    }

    public String getHitPayload() {
        return hitPayload;
    }

    public void setHitPayload(String hitPayload) {
        this.hitPayload = hitPayload;
    }
}
// [END joined_action_model]
