package tech.ytsaurus.flow.examples.waitclickjoin.model;

import javax.persistence.Column;
import javax.persistence.Entity;

// [BEGIN hit_model]
// Important! The field order must match the "hit" stream definition in the static spec.
@Entity
public class Hit {
    @Column(name = "hit_id")
    private String hitId;
    @Column(name = "hit_time", columnDefinition = "uint64")
    private Long hitTime;
    @Column(name = "hit_payload")
    private String hitPayload;

    public Hit() {
    }

    public Hit(String hitId, Long hitTime, String hitPayload) {
        this.hitId = hitId;
        this.hitTime = hitTime;
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

    public String getHitPayload() {
        return hitPayload;
    }

    public void setHitPayload(String hitPayload) {
        this.hitPayload = hitPayload;
    }

    @Override
    public String toString() {
        return "Hit{" +
                "hitId='" + hitId + '\'' +
                ", hitTime=" + hitTime +
                ", hitPayload='" + hitPayload + '\'' +
                '}';
    }
}
// [END hit_model]
