package tech.ytsaurus.flow.examples.waitclickjoin.model;

import javax.persistence.Column;
import javax.persistence.Entity;

// [BEGIN hit_model]
// The stream schema is derived from this class: the runner writes it into the pipeline spec,
// so "hit" needs no schema of its own there. Field order defines column order, and
// columnDefinition pins a column type where the default for the field type differs.
@Entity
public class Hit {
    @Column(name = "hit_id", columnDefinition = "string")
    private String hitId;
    @Column(name = "hit_time", columnDefinition = "uint64")
    private Long hitTime;
    @Column(name = "hit_payload", columnDefinition = "string")
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
