/**
 * A concrete implementation of a relational plan operator representing a join
 * operator.
 * 
 * You should create subclasses of JoinNode as needed to store metadata relevant
 * to specific join types. (e.g., the join key)
 *
 **********************************************************
 * You should not need to modify this file for Project 1. *
 **********************************************************
 */

package edu.buffalo.cse.sql.plan;

import java.util.List;
import java.util.ArrayList;

import edu.buffalo.cse.sql.Schema;

public class JoinNode extends PlanNode.Binary {
  public enum JType { NLJ, MERGE, HASH, INDEX };
  
  protected JType type;
  protected ExprTree condition;
  
  public JoinNode(){ super(PlanNode.Type.JOIN); this.type = JType.NLJ; this.condition=null;}
  
  public ExprTree getCondition() {
	return condition;
}

public void setCondition(ExprTree condition) {
	this.condition = condition;
}

public JType getJoinType(){ return type; }
  public void setJoinType(JType type){ this.type = type; }

  public String detailString(){
    StringBuilder sb = new StringBuilder(super.detailString());
    
    sb.append(" [");
    switch(type) {
      case NLJ: sb.append("NESTED LOOP"); break;
      default:
        sb.append(type.toString());
        break;
    }
    if(condition!=null && !condition.isEmpty()){
    	sb.append("(");
    	sb.append(condition.toString());
    	sb.append(")");
    }
    sb.append("]");
    return sb.toString();
  }

  public List<Schema.Var> getSchemaVars()
  {
    ArrayList<Schema.Var> vars = new ArrayList<Schema.Var>();
    vars.addAll(getLHS().getSchemaVars());
    vars.addAll(getRHS().getSchemaVars());
    return vars;
  }
    
  public static JoinNode make(PlanNode lhs, PlanNode rhs){
    JoinNode j = new JoinNode();
    j.setLHS(lhs); j.setRHS(rhs);
    return j;
  }
}
