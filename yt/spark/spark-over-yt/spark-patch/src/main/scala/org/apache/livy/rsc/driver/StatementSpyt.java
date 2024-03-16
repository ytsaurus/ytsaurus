package org.apache.livy.rsc.driver;

import tech.ytsaurus.spyt.patch.annotations.OriginClass;
import tech.ytsaurus.spyt.patch.annotations.Subclass;

@Subclass
@OriginClass("org.apache.livy.rsc.driver.Statement")
public class StatementSpyt extends Statement {
  private QueryPlan plan = null;

  public StatementSpyt(Integer id, String code, StatementState state, String output) {
    super(id, code, state, output);
  }

  public StatementSpyt() {
    super();
  }

  public void setPlan(QueryPlan p) {
    this.plan = p;
  }

  public QueryPlan getPlan() {
    return plan;
  }
}
