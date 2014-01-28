package edu.buffalo.cse.sql.optimizer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.plan.ExprTree;
import edu.buffalo.cse.sql.plan.ExprTree.VarLeaf;
import edu.buffalo.cse.sql.plan.Expression;
import edu.buffalo.cse.sql.plan.IndexScanNode;
import edu.buffalo.cse.sql.plan.JoinNode;
import edu.buffalo.cse.sql.plan.JoinNode.JType;
import edu.buffalo.cse.sql.plan.PlanNode;
import edu.buffalo.cse.sql.plan.ScanNode;
import edu.buffalo.cse.sql.plan.SelectionNode;

public class PushDownSelects extends PlanRewrite{

	public PushDownSelects(boolean defaultTopDown) {
		super(defaultTopDown);
	}

	@Override
	protected PlanNode apply(PlanNode node) throws SqlException {

		edu.buffalo.cse.sql.plan.PlanNode.Type type=node.type;
		switch(type){
		case AGGREGATE:{
			return node;
		}
		case SELECT:{
			SelectionNode objSelect= (SelectionNode)node;
			List<ExprTree> lscon=(List<ExprTree>) objSelect.conjunctiveClauses();
			Iterator<ExprTree> itcon= lscon.iterator();
			java.util.List<ExprTree> lsremove=new java.util.ArrayList<ExprTree>();
			java.util.List<ExprTree> lsnotremove=new java.util.ArrayList<ExprTree>();
			while(itcon.hasNext()){
				ExprTree exp=itcon.next();
				List ls=new Expression(exp).findColumns();
				switch(exp.op){
				case EQ:{
					if(objSelect.getChild().type==PlanNode.Type.JOIN){
						JoinNode jn=(JoinNode) objSelect.getChild();
						//if(jn.getJoinType()==JType.NLJ){


						if(ls.size()!=1){
							boolean bool=joinS(jn,ls,exp);
							if (bool){
								exp.joinflag=true;
								lsremove.add(exp);
								//}
							}
							else{
								lsnotremove.add(exp);
							}
						}
					}
				}
				case GT:
				case LT:
				case GTE:
				case LTE:
				case NEQ:{
					//List ls=new Expression(exp).findColumns();
					if(ls.size()==1){
						if(Sql.flag_index==1){
							PlanNode node1=objSelect.getChild();
							if(findscan(node1,ls,exp)){
								lsremove.add(exp);
							}
						}
					}
					break;
				}
				}
			}
			if(lsremove.size()!=0){
				Iterator itremove= lsremove.iterator();
				while(itremove.hasNext()){
					ExprTree expremove=(ExprTree) itremove.next();
					if(objSelect.getCondition().equals(expremove)){
						ExprTree expr=null;
						if(lsnotremove.size()!=0){
							expr=lsnotremove.get(0);
						}

						objSelect.setCondition(expr);
					}
					else{
						if(objSelect.getCondition().remove(expremove)){
							if(objSelect.getCondition()!=null || !objSelect.getCondition().isEmpty()){
								ExprTree.OpCode op=objSelect.getCondition().get(0).op;
								objSelect.setCondition(objSelect.getCondition().get(0));
								//objSelect.getCondition().op=op;
								System.out.println("");
							}
						}
						else{
							if(lsnotremove.size()!=0){
								Iterator itnotremove= lsnotremove.iterator();
								while(itnotremove.hasNext()){
									ExprTree expnotremove=(ExprTree) itnotremove.next();
									if(objSelect.getCondition().remove(expnotremove)){
										if(objSelect.getCondition()!=null || !objSelect.getCondition().isEmpty()){
											ExprTree.OpCode op=objSelect.getCondition().get(0).op;
											objSelect.setCondition(objSelect.getCondition().get(0));

											if(objSelect.getCondition().remove(expremove));
											op=objSelect.getCondition().get(0).op;
											objSelect.setCondition(objSelect.getCondition().get(0));
										}
									}
								}
							}
						}
					}
				}
			}
			node=objSelect;
			return node;
		}
		case JOIN:
			return node;
		case NULLSOURCE:
			return node;
		case PROJECT:
			return node;
		case SCAN:{
			ScanNode objScan=(ScanNode)node;
			if(objScan.getCondition()!=null&& !objScan.getCondition().isEmpty() ){

				IndexScanNode objindex=new IndexScanNode(objScan.table, objScan.schema,objScan.getCondition());
				node=objindex ;
			}
		}
		return node;
		case UNION:
			return node;
		default:
			break;

		}
		return null;
	}
	//	public boolean joinsuitable(PlanNode pn, List<VarLeaf> ls){
	//		boolean bool=false;
	//		java.util.List<Var> ls1= new java.util.ArrayList<Schema.Var>();
	//		Iterator<VarLeaf> it=null;
	//		List<Var> lsvar=pn.getSchemaVars();
	//		Iterator<Var> it1=lsvar.iterator();
	//		while(it1.hasNext()){
	//			Schema.Var var1=it1.next();
	//			it=ls.iterator();
	//			while(it.hasNext()){
	//				VarLeaf var2= it.next();
	//				if(var1.equals(var2.name)){
	//					bool=true;
	//				}
	//			}
	//		}
	//		//		if(pn.type==PlanNode.Type.SCAN){
	//		//			ScanNode sc=(ScanNode)pn;
	//		//			List<Var> lsvar=sc.getSchemaVars();
	//		//			Iterator<Var> it1=lsvar.iterator();
	//		//			while(it1.hasNext()){
	//		//				Schema.Var var1=it1.next();
	//		//					it=ls.iterator();
	//		//				while(it.hasNext()){
	//		//					VarLeaf var2= it.next();
	//		//				if(var1.equals(var2.name)){
	//		//					bool=true;
	//		//				}
	//		//					
	//		//				}
	//		//			}
	//		//			return bool;
	//		//		}
	//		//		else{
	//		//			JoinNode jn=(JoinNode)pn;
	//		//			return (joinsuitable(jn.getLHS(),ls)||joinsuitable(jn.getRHS(),ls));
	//		//		}
	//		return bool;
	//	}
	public boolean joinS(JoinNode jn,List ls,ExprTree exp){
		boolean bool=false;
		PlanNode p1=jn.getLHS();
		PlanNode p2=jn.getRHS();
		List<Var> lsvar=p1.getSchemaVars();
		List<Var> lsvar1=p2.getSchemaVars();
		if(joinsuitable(lsvar,ls)){
			if(joinsuitable(lsvar1,ls)){
				if(jn.getCondition()==null){
					jn.setJoinType(JType.HASH);
					jn.setCondition(exp);
					bool=true;
				}
			}

		}
		if(!bool){
			boolean bool1=false;
			boolean bool2=false;
			if(p1.type==PlanNode.Type.JOIN){
				bool1=joinS((JoinNode)p1,ls,exp);
			}
			if(p2.type==PlanNode.Type.JOIN){
				bool2=joinS((JoinNode)p2,ls,exp);
			}
			if(bool1||bool2)
				bool=true;

		}
		return bool;
	}
	public boolean joinsuitable(List<Var> lsvar, List<VarLeaf> ls){
		boolean bool=false;
		Iterator<VarLeaf> it=null;
		Iterator<Var> it1=lsvar.iterator();
		while(it1.hasNext()){
			Schema.Var var1=it1.next();
			it=ls.iterator();
			while(it.hasNext()){
				VarLeaf var2= it.next();
				if(var1.equals(var2.name)){
					bool=true;
				}
			}
		}
		//		if(pn.type==PlanNode.Type.SCAN){
		//			ScanNode sc=(ScanNode)pn;
		//			List<Var> lsvar=sc.getSchemaVars();
		//			Iterator<Var> it1=lsvar.iterator();
		//			while(it1.hasNext()){
		//				Schema.Var var1=it1.next();
		//					it=ls.iterator();
		//				while(it.hasNext()){
		//					VarLeaf var2= it.next();
		//				if(var1.equals(var2.name)){
		//					bool=true;
		//				}
		//					
		//				}
		//			}
		//			return bool;
		//		}
		//		else{
		//			JoinNode jn=(JoinNode)pn;
		//			return (joinsuitable(jn.getLHS(),ls)||joinsuitable(jn.getRHS(),ls));
		//		}
		return bool;
	}
	public boolean findscan(PlanNode node1,List<VarLeaf> ls,ExprTree exp){
		boolean bool=false;
		boolean bool1=false;
		boolean bool2=false;
		if(node1.type==PlanNode.Type.JOIN){
			JoinNode objJoin=(JoinNode) node1;
			bool1=findscan(objJoin.getLHS(),ls,exp);
			bool2=findscan(objJoin.getRHS(),ls,exp);
			if(bool1||bool2){
				bool=true;
			}
		}
		else if(node1.type==PlanNode.Type.SCAN){
			ScanNode objScan=(ScanNode)node1;
			List<Var> lsvar=node1.getSchemaVars();
			Iterator<Var> it=lsvar.iterator();
			Var v1= ls.get(0).name;
			while(it.hasNext()){
				Var v= it.next();
				if(v.equals(v1)){
					List lscond= objScan.getCondition();
					if(lscond!=null && !lscond.isEmpty())
						lscond.add(exp);
					else{
						lscond=new ArrayList();
						lscond.add(exp);
					}
					objScan.setCondition(lscond);
					bool=true;
				}
			}				
		}

		return bool;
	}


}
