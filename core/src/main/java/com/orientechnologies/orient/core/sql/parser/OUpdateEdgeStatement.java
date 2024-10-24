/* Generated By:JJTree: Do not edit this line. OUpdateEdgeStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OUpdateExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OUpdateExecutionPlanner;
import java.util.stream.Collectors;

public class OUpdateEdgeStatement extends OUpdateStatement {
  public OUpdateEdgeStatement(int id) {
    super(id);
  }

  public OUpdateEdgeStatement(OrientSql p, int id) {
    super(p, id);
  }

  protected String getStatementType() {
    return "UPDATE EDGE ";
  }

  @Override
  public OUpdateExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    OUpdateExecutionPlanner planner = new OUpdateExecutionPlanner(this);
    OUpdateExecutionPlan result = planner.createExecutionPlan(ctx, enableProfiling);
    result.setStatement(originalStatement);
    result.setGenericStatement(this.toGenericStatement());
    return result;
  }

  @Override
  public OUpdateEdgeStatement copy() {
    OUpdateEdgeStatement result = new OUpdateEdgeStatement(-1);
    result.target = target == null ? null : target.copy();
    result.operations =
        operations == null
            ? null
            : operations.stream().map(x -> x.copy()).collect(Collectors.toList());
    result.upsert = upsert;
    result.returnBefore = returnBefore;
    result.returnAfter = returnAfter;
    result.returnProjection = returnProjection == null ? null : returnProjection.copy();
    result.whereClause = whereClause == null ? null : whereClause.copy();
    result.limit = limit == null ? null : limit.copy();
    result.timeout = timeout == null ? null : timeout.copy();
    return result;
  }
}
/* JavaCC - OriginalChecksum=496f32976ee84e3a3a89d1410dc134c5 (do not edit this line) */
