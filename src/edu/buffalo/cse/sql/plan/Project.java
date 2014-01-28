package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.util.ManageList;
import edu.buffalo.cse.sql.util.Utility;

public class Project extends ProjectionNode {
	public ProjectionNode pnode;
	List<Datum[]> dataForExp=new ArrayList<Datum[]>();
	List<Schema.Var> schemaForExp = new ArrayList<Schema.Var>();
	
	public Project(ProjectionNode pnode){
		this.pnode=pnode;
	}
	public ProjectionNode getProjectionNode(){
		return pnode;
	}
	
	public List<Datum[]> doProject() throws CastError{
		dataForExp = Utility.switchNodes(pnode.getChild());
		schemaForExp = pnode.getChild().getSchemaVars(); 
		Iterator<ProjectionNode.Column> it = pnode.columns.iterator();	//Iterator on project columns
		schemaForExp.isEmpty();
		ProjectionNode.Column col;
		col=it.next();
		List<Datum[]> lsdatum = null;
		List<Datum[]> finalList= new ArrayList<Datum[]>();
		lsdatum = new Expression(col.expr,dataForExp,schemaForExp).doExpr();
		ManageList mg= null;
		Datum[] arrdatum= null;
		for( int i=0;i<lsdatum.size();i++){
			arrdatum=new Datum[pnode.columns.size()];
			arrdatum[0] =lsdatum.get(i)[0];
			finalList.add(arrdatum);
		}
		int j=1;
		while(it.hasNext()){
			col=it.next();
			lsdatum = new Expression(col.expr,dataForExp,schemaForExp).doExpr();
			mg= new ManageList(lsdatum);
			for( int i=0;i<lsdatum.size();i++){
				finalList.get(i)[j]= mg.getDatum(i,0);
			}
			j=j+1;	
		}
		return finalList;
	}
	
	public List findcolumns(){
		//pnode.getColumns();
		Iterator<ProjectionNode.Column> it = pnode.columns.iterator();
		ProjectionNode.Column col;
		List ls=new ArrayList();
		if(!(pnode.getChild().type==PlanNode.Type.SCAN)){
		ls=Utility.findCol(pnode.getChild());
		}
		while(it.hasNext()){
			col=it.next();
			ExprTree exp=(ExprTree) col.expr;
			List ls2=new Expression(exp).findColumns();
			Iterator itls= ls2.iterator();
			while(itls.hasNext()){
				ExprTree.VarLeaf vf=(ExprTree.VarLeaf)itls.next();
				ls.add(vf);
			}
	
		}
		return ls;
		
	}

}
