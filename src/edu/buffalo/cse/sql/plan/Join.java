package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.ExprTree.VarLeaf;
import edu.buffalo.cse.sql.util.Utility;

public class Join extends JoinNode {
	public JoinNode join;

	public Join(JoinNode join){
		super();
		this.join=join;
	}
	public List<Datum[]> doJoin() throws CastError{
		PlanNode LHSnode=join.getLHS();
		PlanNode RHSnode=join.getRHS();
		List<Datum[]> lsLHSDatum=Utility.switchNodes(LHSnode);
		List<Datum[]> lsRHSDatum=Utility.switchNodes(RHSnode);
		Datum[] arrLHSDatum=null; //num columns
		Datum[] arrRHSDatum=null;
		List<Datum[]> lsfinalDatum= new ArrayList<Datum[]>();
		if(join.type!=JType.HASH){
			int LHSsize= lsLHSDatum.size();
			int RHSsize=lsRHSDatum.size();

			for( int i=0;i<LHSsize;i++){
				arrLHSDatum=new Datum[LHSsize];
				arrLHSDatum= lsLHSDatum.get(i);
				int arrLHSsize= arrLHSDatum.length;
				for(int j=0;j<RHSsize;j++){
					arrRHSDatum=new Datum[RHSsize];
					arrRHSDatum= lsRHSDatum.get(j);
					int arrRHSsize= arrRHSDatum.length;
					Datum arrnewDatum[] = new Datum[arrLHSsize+arrRHSsize];
					for(int k=0;k<arrLHSsize;k++){
						arrnewDatum[k]=arrLHSDatum[k];
					}
					for(int n=0;n<arrRHSsize;n++){
						arrnewDatum[n+arrLHSsize]=arrRHSDatum[n];
					}
					lsfinalDatum.add(arrnewDatum);
				}
			}
		}
		else{	//code for hybrid hash join
			ExprTree exp=join.getCondition();
			exp.joinflag=true;
			List ls=new Expression(exp).findColumns();
			List<Schema.Var> lsvar1=join.getLHS().getSchemaVars();
			System.out.println();
			List<Schema.Var> lsvar2= join.getRHS().getSchemaVars();
			Iterator<Var> iter1=lsvar1.iterator();
			Iterator<Var> iter3=lsvar2.iterator();
			Iterator iter2=ls.iterator();
			int j=-1;
			int index1=-1;
			int index2=-1;
			while(iter2.hasNext()){
				ExprTree.VarLeaf varleaf1= (VarLeaf) iter2.next();
				Schema.Var var1=varleaf1.name;
				j=-1;
				iter1=lsvar1.iterator();
				while(iter1.hasNext()){
					j=j+1;
					Schema.Var var2=iter1.next();
					if(Sql.tablemap.get(var2.rangeVariable)!=null)
					if(Sql.tablemap.get(var2.rangeVariable).equals(var1.rangeVariable))
						var1.rangeVariable=var2.rangeVariable;
					if(var1.equals(var2)){
						index1=j;
						break;
					}

				}
				j=-1;
				iter3=lsvar2.iterator();
				while(iter3.hasNext()){
					j=j+1;
					Schema.Var var2=iter3.next();
					if(Sql.tablemap.get(var2.rangeVariable)!=null)
					if(Sql.tablemap.get(var2.rangeVariable).equals(var1.rangeVariable))
						var1.rangeVariable=var2.rangeVariable;
					if(var1.equals(var2)){
						index2=j;
						break;
					}

				}
			}
			lsfinalDatum=doHybridHashJoin(lsLHSDatum, lsRHSDatum, index1, index2);

		}
		return lsfinalDatum;
	}
	public static List<Datum[]> doHybridHashJoin(List<Datum[]> lhs, List<Datum[]> rhs, int i, int j) throws CastError{
		//lhs.get(0)[i].getType()
		int idx1,idx2;
		List<Datum[]> primary = new ArrayList<Datum[]>();
		List<Datum[]> secondary = new ArrayList<Datum[]>();
		List<Datum[]> lsfinalDatum = new ArrayList<Datum[]>();
		//int r=lhs.get(0).length;
		//int m=rhs.get(0).length;
		if(lhs.size()>=rhs.size()){
			primary = lhs;
			secondary = rhs;
			idx1 = i;
			idx2 = j;
		}
		else{
			primary = rhs;
			secondary = lhs;
			idx1 = j;
			idx2 = i;
		}
		HashMap<Datum, ArrayList<Datum[]>> hhj = new HashMap<Datum, ArrayList<Datum[]>>();
		Iterator<Datum[]> it1 = primary.iterator();
		int k2=0;
		int num=0;
		while(it1.hasNext()){			//Putting values in HashMap
			num=num+1;
//			if(num==k2+1000){
//				k2=k2+1000;
//				System.out.println("I am here:" + num );
//			}
			Datum[] row = it1.next();
			if(hhj.containsKey(row[idx1])){
				ArrayList<Datum[]> temp = hhj.get(row[idx1]);
				temp.add(row);
			}
			else{
				ArrayList<Datum[]> rowHolder = new ArrayList<Datum[]>();
				rowHolder.add(row);
				hhj.put(row[idx1], rowHolder);
			}
		}
		Iterator<Datum[]> it2 = secondary.iterator();
		num=0;
		int k1=0;
		//Datum[] res = new Datum[r+m];
		while(it2.hasNext()){	
			Datum[] row = it2.next();
			Datum[] res = new Datum[primary.size()+secondary.size()];
			num=num+1;
			
			if(hhj.containsKey(row[idx2])){
				ArrayList<Datum[]> temp = hhj.get(row[idx2]);
				Iterator<Datum[]> it3 = temp.iterator();
				while(it3.hasNext()){
//					if(num==k1+1000){
//						k1=k1+1000;
//						System.out.println("Join:" + num);
//					}
					
					Datum[] matchedRow = it3.next();
					if(lhs.size()>=rhs.size()){
						res= new Datum[row.length + matchedRow.length];
						for(int k=0;k<matchedRow.length;k++)
							res[k] = matchedRow[k];
						for(int n=0;n<row.length;n++)
							res[n+matchedRow.length] = row[n];
						lsfinalDatum.add(res);
						res=null;
						matchedRow=null;
					}
					else{
						int s=row.length + matchedRow.length;
						res= new Datum[s];
						for(int k=0;k<row.length;k++)
							res[k] = row[k];
						for(int n=0;n<matchedRow.length;n++)
							res[n+row.length] = matchedRow[n];
						lsfinalDatum.add(res);
						res=null;
						matchedRow=null;
					}
				}			
			}
			res=null;
			row=null;
		}

		return lsfinalDatum;
	}
	public List findcolumns(){
		//pnode.getColumns();
		ExprTree cond = join.condition;
		List ls=new ArrayList();
		List lslhs=new ArrayList();
		List lsrhs=new ArrayList();
		if(cond!=null && !cond.isEmpty()){
			List ls1=new Expression(cond).findColumns();
			Iterator lsit= ls1.iterator();
			while(lsit.hasNext()){
				ls.add(lsit.next());
			}
		}
		if(!(join.getLHS().type==PlanNode.Type.SCAN)){
			lslhs=Utility.findCol(join.getLHS());
		}
		if(!(join.getRHS().type==PlanNode.Type.SCAN)){
			lsrhs=Utility.findCol(join.getRHS());
		}
		
//		Iterator itlhs= lslhs.iterator();
//		Iterator itrhs=lsrhs.iterator();
//		while(itlhs.hasNext()){
//			ls.add(itlhs.next());
//		}
//		while(itrhs.hasNext()){
//			ls.add(itrhs.next());
//		}
		ls.addAll(lslhs);
		ls.addAll(lsrhs);
		return ls;

	}

}
