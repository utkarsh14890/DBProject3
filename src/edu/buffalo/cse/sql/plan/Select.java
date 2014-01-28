package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.util.Utility;

public class Select extends SelectionNode{
	public SelectionNode s;
	List<Datum[]> dataForExp=new ArrayList<Datum[]>();
	List<Schema.Var> schemaForExp = new ArrayList<Schema.Var>();

	public Select(SelectionNode s) {
		super(s.condition);
		this.s=s;
		
	}


	public SelectionNode getS() {
		return s;
	}


	public void setS(SelectionNode s) {
		this.s = s;
	}


	public List<Datum[]> doSelect() throws CastError{
		dataForExp = Utility.switchNodes(s.getChild());
		schemaForExp = s.getChild().getSchemaVars(); 
		System.out.println();
		if(s.condition!=null && !s.condition.isEmpty()){
			List<Datum[]> lsdatum = new Expression(s.condition,dataForExp,schemaForExp).doExpr();
			
			return lsdatum;
			
		}
		else{
			return dataForExp;
		}

	}
	public List findcolumns(){
		//pnode.getCoasdasdasasdasdasdasdasdasdddddddddddddddddddddddasdasdlumns();
		ExprTree cond = s.condition;
		List ls=new ArrayList();
		if(cond!=null && !cond.isEmpty()){
		List ls1=new Expression(cond).findColumns();
		Iterator lsit= ls1.iterator();
		while(lsit.hasNext()){
			ls.add(lsit.next());
		}
		}
		if(!(s.getChild().type==PlanNode.Type.SCAN)){
		List ls_child=Utility.findCol(s.getChild());
		ls.addAll(ls_child);
		
		}
		return ls;

	}

}
