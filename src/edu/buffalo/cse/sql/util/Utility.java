package edu.buffalo.cse.sql.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.Schema.Column;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.Aggregate;
import edu.buffalo.cse.sql.plan.AggregateNode;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.IndexScan;
import edu.buffalo.cse.sql.plan.IndexScanNode;
import edu.buffalo.cse.sql.plan.Join;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.Null;
import edu.buffalo.cse.sql.plan.NullSourceNode;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.Project;
import edu.buffalo.cse.sql.plan.ProjectionNode;
import edu.buffalo.cse.sql.plan.Scan;
import edu.buffalo.cse.sql.plan.PlanNode.Type;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.Select;
import edu.buffalo.cse.sql.plan.SelectionNode;
import edu.buffalo.cse.sql.plan.Union;
import edu.buffalo.cse.sql.plan.UnionNode;

public class Utility {

	public static List<Datum[]> switchNodes(PlanNode node) throws CastError{
		List<Datum[]> lsDatum= new ArrayList<Datum[]>(); 
		Type type= node.type;
		switch(type){
		case SELECT:{
			Select objSelect= new Select((SelectionNode)node);
			lsDatum=objSelect.doSelect();
			break;
		}

		case PROJECT:{
			Project objProject= new Project((ProjectionNode)node);
			lsDatum=objProject.doProject();
			break;
		}
		case JOIN:{
			Join objJoin = new Join((JoinNode)node);
			lsDatum=objJoin.doJoin();
			break;
		}
		case SCAN:{
			Scan objScan=new Scan((ScanNode)node);
			lsDatum=objScan.doScan();
			break;
		}
		case NULLSOURCE:{
			Null objNullSource= new Null((NullSourceNode)node);
			lsDatum=objNullSource.doNull();
			break;
		}
		case UNION:{
			Union objUnion=new Union((UnionNode)node);
			lsDatum=objUnion.doUnion();
			break;
		}
		case AGGREGATE:{
			Aggregate objAggregate= new Aggregate((AggregateNode)node);
			lsDatum=objAggregate.doAggregate();
			break;
		}
		case INDEXSCAN:{
			IndexScan objScan=new IndexScan((IndexScanNode)node);
			lsDatum=objScan.doIndexScan();
			break;
		}
		}
		return lsDatum;
	}
	
	public static List findCol(PlanNode node){
		Type type= node.type;
		List col=new ArrayList();
		switch(type){
		case PROJECT:{
			Project objProject= new Project((ProjectionNode)node);
			col=objProject.findcolumns();
			//System.out.println("hello");
			break;
		}
		case SELECT:{
			Select objSelect= new Select((SelectionNode)node);
			col=objSelect.findcolumns();
			
			//System.out.println("hello");
			break;
		}
		case AGGREGATE:{
			Aggregate objAggregate= new Aggregate((AggregateNode)node);
			col=objAggregate.findcolumns();
			//System.out.println("hello");
			break;
		}
		case JOIN:{
			Join objJoin= new Join((JoinNode)node);
			col=objJoin.findcolumns();
			break;
		}
		case UNION:{
			Union objUnion= new Union((UnionNode)node);
			col=objUnion.findcolumns();
			break;
		}
		case INDEXSCAN:{
			IndexScan objIndexScan=new IndexScan((IndexScanNode)node);
			col=objIndexScan.findcolumns();
			break;
		}
		}
		return col;
	}
	
	public static List<ExprTree> findIndexNodes(PlanNode node){
	
		
		
		Type type= node.type;
	
		List col=new ArrayList();
		List<ExprTree> ls= new ArrayList();
		switch(type){
		case PROJECT:{
			Project objProject= new Project((ProjectionNode)node);
			ls=findIndexNodes(objProject.pnode.getChild());
			break;
		}
		case SELECT:{
			Select objSelect= new Select((SelectionNode)node);
			ls=findIndexNodes(objSelect.s.getChild());
			break;
		}
		case AGGREGATE:{
			Aggregate objAggregate= new Aggregate((AggregateNode)node);
			PlanNode node1=objAggregate.a.getChild();
			ls=Utility.findIndexNodes(node1);
			break;
		}
		case JOIN:{
			Join objJoin= new Join((JoinNode)node);
			List<ExprTree> ls1=findIndexNodes(objJoin.join.getLHS());
			List<ExprTree> ls2=findIndexNodes(objJoin.join.getRHS());
			ls1.addAll(ls2);
			ls=ls1;
			break;
		}
		case UNION:{
			Union objUnion= new Union((UnionNode)node);
			List<ExprTree> ls1=findIndexNodes(objUnion.unode.getLHS());
			List<ExprTree> ls2=findIndexNodes(objUnion.unode.getRHS());
			ls1.addAll(ls2);
			ls=ls1;
			break;
		}
		case INDEXSCAN:{
			IndexScan objIndexScan=new IndexScan((IndexScanNode)node);
			ls=objIndexScan.getCondition();
			
			break;
		}
		}
		return ls;
	}
}
