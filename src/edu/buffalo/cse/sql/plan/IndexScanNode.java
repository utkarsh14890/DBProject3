/**
 * A concrete implementation of a relational plan operator representing a file
 * scan operator.
 *
 **********************************************************
 * You should not need to modify this file for Project 1. *
 **********************************************************
 */
package edu.buffalo.cse.sql.plan;

import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Sql;

public class IndexScanNode extends PlanNode.Leaf {
  public final String table;
  public final Schema.Table schema;
  public List<ExprTree> condition;
  boolean flag_setSchemaVar=false;
  
  public List<ExprTree> getCondition() {
	return condition;
}
public void setCondition(List<ExprTree> condition) {
	this.condition = condition;
}
public IndexScanNode(String table, String rangeVariable, Schema.Table schema,List<ExprTree> condition) 
  { 
    super(PlanNode.Type.INDEXSCAN); 
    this.table = table; 
    this.schema = schema.changeRangeVariable(rangeVariable);
    this.condition=condition;
    if(!Sql.tablemap.containsKey(table))
		Sql.tablemap.put(table, rangeVariable);
  }
  public IndexScanNode(String table, Schema.Table schema,List<ExprTree> condition) 
  {
    this(table, table, schema,condition);
  }

  public String detailString(){
    StringBuilder sb = new StringBuilder(super.detailString());
    String sep = "";
    
    sb.append(" [");
    if(condition.size()>0){
    	Iterator<ExprTree> itcondition= condition.iterator();
    	while(itcondition.hasNext()){
    		sb.append(sep);
    		sb.append(itcondition.next().toString());
    		 sep = ", ";
    	}
    	sb.append(";");
    }
    sep = "";
    sb.append(table);
    sb.append("(");
    for(Schema.Var v : getSchemaVars()){
      sb.append(sep);
      sb.append(v.name);
      sep = ", ";
    }
    sb.append(")]");
    
    return sb.toString();
  }

  public List<Schema.Var> getSchemaVars()
  {
	
	  //System.out.println("hi_test on git");
	if(!flag_setSchemaVar && Sql.flag_hmp_tables_col_used){
			setSchemaVars();
			flag_setSchemaVar=true;
		}
	  
    List<Schema.Var> vars = new ArrayList<Schema.Var>();
    for(Schema.Column c : schema){
      vars.add(c.name);
    }
    return vars;
  }
  
  public void setSchemaVars(){
		List<String> ls_col_name=new ArrayList<String>();
		
		if(Sql.hmp_tables_col_used.get(Sql.tablemap.get(table))!=null)		
		ls_col_name.addAll(Sql.hmp_tables_col_used.get(Sql.tablemap.get(table)));
		
		if(Sql.hmp_tables_col_used.get("nothing")!=null)
		ls_col_name.addAll(Sql.hmp_tables_col_used.get("nothing"));
	
		
		Iterator<Schema.Column> it_schema=schema.iterator();
		while(it_schema.hasNext()){
			if(!ls_col_name.contains(it_schema.next().name.name)){
				it_schema.remove();		
			}
		}
	}
}
