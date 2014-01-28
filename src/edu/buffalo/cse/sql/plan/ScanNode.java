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

public class ScanNode extends PlanNode.Leaf {
	public final String table;
	public final Schema.Table schema;
	protected List<ExprTree> condition;
	boolean flag_setSchemaVar=false;
	
	public ScanNode(String table, String rangeVariable, Schema.Table schema) 
	{ 
		super(PlanNode.Type.SCAN); 
		this.table = table; 
		this.schema = schema.changeRangeVariable(rangeVariable);
		this.condition=null;
		if(!Sql.tablemap.containsKey(table))
		Sql.tablemap.put(table, rangeVariable);
	}
	public ScanNode(String table, Schema.Table schema) 
	{
		this(table, table, schema);
	}
	public List<ExprTree> getCondition() {
//		if(!flag_setSchemaVar && Sql.flag_hmp_tables_col_used){
//			setSchemaVars();
//			flag_setSchemaVar=true;
//		}
		return condition;
	}
	public void setCondition(List<ExprTree> condition) {
//		if(!flag_setSchemaVar && Sql.flag_hmp_tables_col_used){
//			setSchemaVars();
//			flag_setSchemaVar=true;
//		}
		this.condition = condition;
	}
	

	public String detailString(){
//		if(!flag_setSchemaVar && Sql.flag_hmp_tables_col_used){
//			setSchemaVars();
//			flag_setSchemaVar=true;
//		}
		StringBuilder sb = new StringBuilder(super.detailString());
		String sep = "";

		sb.append(" [");
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
		
		if(!flag_setSchemaVar && Sql.flag_hmp_tables_col_used){
			//System.out.println("doing set Schema Vars");
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
		
//		if(Sql.hmp_tables_col_used.get(table)!=null)		
//		ls_col_name.addAll(Sql.hmp_tables_col_used.get(table));
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
