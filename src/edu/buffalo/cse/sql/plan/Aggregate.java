package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
//import java.util.Collections;
import java.util.Iterator;
import java.util.List;


import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.Sql;
//import edu.buffalo.cse.sql.Schema.Var;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.plan.ExprTree.VarLeaf;
import edu.buffalo.cse.sql.plan.ProjectionNode.Column;
import edu.buffalo.cse.sql.util.Utility;

public class Aggregate extends AggregateNode{
	public AggregateNode a;
	List<Datum[]> dataForExp=new ArrayList<Datum[]>();
	List<Schema.Var> schemaForExp = new ArrayList<Schema.Var>();

	public Aggregate(AggregateNode a){
		this.a=a;		//save the node
	}
	public List<Datum[]> doAggregate() throws CastError{
//		System.out.println("hello");
		dataForExp = Utility.switchNodes(a.getChild());
		schemaForExp = a.getChild().getSchemaVars(); 
		//System.out.println(schemaForExp.isEmpty());
		Iterator<AggColumn> it = a.aggregates.iterator();	//Iterator on aggregates
		int size= a.aggregates.size();
		AggColumn x;
		Datum[]res=new Datum[size];
		List<Datum[]> lsdatum= new ArrayList<Datum[]>();
		int i=0;
		List<Column> arlsGroup=a.groupByVars;
		Iterator<Column> itGroup=arlsGroup.iterator();
		Column g;
		HashMap<List<Datum>,List<Datum[]>> hmp=new HashMap<List<Datum>,List<Datum[]>>();
		boolean bool=false;

		List<Integer> lsindex=new ArrayList<Integer>();
		while(itGroup.hasNext()){
			bool=true;
			g=itGroup.next();
			ExprTree e=g.expr;
			ExprTree.VarLeaf vf= (ExprTree.VarLeaf)e;
			//vf.name.rangeVariable=Sql.tablemap.keySet().
			//Schema.Var schema=vf.name;
			
			//int index=schemaForExp.indexOf(vf);
			int index=0;
			Iterator<Var> iter=schemaForExp.iterator();
			int j=0;
			while(iter.hasNext()){
				Schema.Var var1=iter.next();
				if(Sql.tablemap.get(var1.rangeVariable)!=null)
				if(Sql.tablemap.get(var1.rangeVariable).equals(vf.name.rangeVariable))
					vf.name.rangeVariable=var1.rangeVariable;
				if(var1.equals(vf.name))
					index=j;
				j++;
			}
			lsindex.add(index);
		}
		if(bool==true){
			Iterator<Datum[]> it_data=dataForExp.iterator();
			while(it_data.hasNext()){
				Datum[] arr=it_data.next();
				ArrayList<Datum> arlsdatum= new ArrayList<Datum>();
				for( int ls=0;ls<lsindex.size();ls++){
					Datum d=(Datum) arr[(Integer) lsindex.get(ls)];
					arlsdatum.add(d);
				}
				//Datum d=arr[index];
				if(hmp.get(arlsdatum)==null){
					List<Datum[]> list= new ArrayList<Datum[]>();
					list.add(arr);
					hmp.put(arlsdatum, list);
				}
				else if(hmp.get(arlsdatum)!=null){
					List<Datum[]> list=(List<Datum[]>) hmp.get(arlsdatum);
					list.add(arr);
					hmp.remove(arlsdatum);
					hmp.put(arlsdatum, list);
				}
			}
		}
		if(bool ==true){
			Iterator hitr=hmp.entrySet().iterator();
			int hsize=hmp.size();
			while(hitr.hasNext()){
				
				int k=0;
				it = a.aggregates.iterator();
				
				Map.Entry pairs=(Map.Entry) hitr.next();
				List ls=(List) pairs.getValue();
				List ll=(List) pairs.getKey();
				int size1=ll.size();
				res=new Datum[size+size1];
				dataForExp=ls;
				
				for(int lll=0;lll<size1;lll++){
				res[k++]=(Datum) ll.get(lll);
				}
				while(it.hasNext()){
					x=it.next();
					ExprTree t= x.expr;		//Save the Expression Tree

					List<Datum[]> l = new Expression(t,dataForExp,schemaForExp).doExpr();
					

					switch(x.aggType){		//Check for the type of aggregates object
					case SUM:
						//int i=0;
						//List<Datum[]>res=new ArrayList<Datum[]>();
						Iterator<Datum[]> is = l.iterator();
						if(l.get(0)[0].getType() == Schema.Type.INT){
							int r = 0;
							while(is.hasNext()){
								r = r + is.next()[0].toInt();
								//l.remove(i);
								//i+=1;
								//System.out.println("hello");
							}
							Datum sendsum =null;
							sendsum=new Datum.Int(r);
							//res[k++]=(Datum) pairs.getKey(); 
							res[k++]=sendsum;
						}
						if(l.get(0)[0].getType() == Schema.Type.FLOAT){
							float r = 0;
							while(is.hasNext()){
								r = r + is.next()[0].toFloat();
								//l.remove(i);
								//i+=1;
							}
							Datum sendsum = null;
							sendsum=new Datum.Flt(r);
							
							res[k++]=sendsum;
						}
						//				else{
						//					System.out.println("Error");
						//				}
						//return res;
						break;
					case COUNT:
						//ExprTree tc= x.expr;
						//List<Datum[]> lc = ExpressionTree.doExpression(tc);
						//List<Datum[]>res_c=new ArrayList<Datum[]>();
						Iterator<Datum[]> ic = l.iterator();
						int count=0;
						while(ic.hasNext()){
							ic.next();
							count++;
						}
						Datum sendcount = null;
						sendcount = new Datum.Int(count);
						//res[k++]=(Datum) pairs.getKey(); 
						//res[k++]=sendsum;
						res[k++]=sendcount;
						//return res_c;
						break;
					case AVG:
						//ExprTree ta = x.expr;
						//List<Datum[]> la = ExpressionTree.doExpression(ta);
						//List<Datum[]>res_a=new ArrayList<Datum[]>();
						Iterator<Datum[]> ia = l.iterator();
						int sumi=0,counter=0;
						float sumf=0;
						float average=0;
						if(l.get(0)[0].getType() == Schema.Type.INT){
							while(ia.hasNext()){	
								sumi+=ia.next()[0].toInt();
								counter++;
							}
							average = (float)sumi/(float)counter;
						}
						if(l.get(0)[0].getType() == Schema.Type.FLOAT){
							while(ia.hasNext()){
								sumf+=ia.next()[0].toFloat();
								counter++;
							}
							average = sumf/counter;			
						}
						Datum sendavg = null;
						sendavg = new Datum.Flt(average);
						//res[k++]=(Datum) pairs.getKey(); 
						res[k++]=sendavg;
						//return res_a;
						break;
					case MAX:
						//ExprTree tmax = x.expr;
						//List<Datum[]> lmax = ExpressionTree.doExpression(tmax);
						//List<Datum[]>res_max = new ArrayList<Datum[]>();
						Iterator<Datum[]> imax = l.iterator();
						int maxi=0;
						float maxf=0;
						if(l.get(0)[0].getType() == Schema.Type.INT){
							List<Integer> a= new ArrayList<Integer>();
							//int j=0;
							while(imax.hasNext()){
								a.add(imax.next()[0].toInt());
							}
							maxi = maxInt(a);
							Datum sendmax = null;
							sendmax=new Datum.Int(maxi);
							//res[k++]=(Datum) pairs.getKey(); 
							res[k++]=sendmax;
						}
						if(l.get(0)[0].getType() == Schema.Type.FLOAT){
							List<Float> a = new ArrayList<Float>();
							//int j = 0;
							while(imax.hasNext()){
								a.add(imax.next()[0].toFloat());
							}
							maxf = maxFloat(a);
							Datum sendmax = null;
							sendmax=new Datum.Flt(maxf);
							//res[k++]=(Datum) pairs.getKey(); 
							res[k++]=sendmax;
						}
						//return res_max;
						break;
					case MIN:
						//ExprTree tmin = x.expr;
						//List<Datum[]> lmin = ExpressionTree.doExpression(tmin);
						//List<Datum[]>res_min = new ArrayList<Datum[]>();
						Iterator<Datum[]> imin = l.iterator();
						int mini=0;
						float minf=0;
						if(l.get(0)[0].getType() == Schema.Type.INT){
							List<Integer> a= new ArrayList<Integer>();
							//int j=0;
							while(imin.hasNext()){
								a.add(imin.next()[0].toInt());
							}
							mini = minInt(a);
							Datum sendmin = null;
							sendmin=new Datum.Int(mini);
							//res[k++]=(Datum) pairs.getKey(); 
							res[k++]=sendmin;
						}
						if(l.get(0)[0].getType() == Schema.Type.FLOAT){
							List<Float> a= new ArrayList<Float>();
							//int j = 0;
							while(imin.hasNext()){
								a.add(imin.next()[0].toFloat());
							}
							minf = minFloat(a);
							Datum sendmin = null;
							sendmin=new Datum.Flt(minf);
							//res[k++]=(Datum) pairs.getKey(); 
							res[k++]=sendmin;
						}
						//return res_min;
						break;
					default:
						return null;

					}

				}
				lsdatum.add(res);
			}
		}
		else{
			while(it.hasNext()){
				x=it.next();
				ExprTree t= x.expr;		//Save the Expression Tree
				//Schema.Type type=dataForExp.get(0)[0].getType();
				//List<Datum[]> l = ExpressionTree.doExpression(t,dataForExp,schemaForExp);	//ExpressionTree class returns 
				List<Datum[]> l = new Expression(t,dataForExp,schemaForExp).doExpr();


				switch(x.aggType){		//Check for the type of aggregates object
				case SUM:
					//int i=0;
					//List<Datum[]>res=new ArrayList<Datum[]>();
					Iterator<Datum[]> is = l.iterator();
					if(l.get(0)[0].getType() == Schema.Type.INT){
						int r = 0;
						while(is.hasNext()){
							r = r + is.next()[0].toInt();
							//l.remove(i);
							//i+=1;
							//System.out.println("hello");
						}
						Datum sendsum =null;
						sendsum=new Datum.Int(r);
						res[i++]=sendsum;
					}
					if(l.get(0)[0].getType() == Schema.Type.FLOAT){
						float r = 0;
						while(is.hasNext()){
							r = r + is.next()[0].toFloat();
							//l.remove(i);
							//i+=1;
						}
						Datum sendsum = null;
						sendsum=new Datum.Flt(r);
						res[i++]=sendsum;
					}
					//				else{
					//					System.out.println("Error");
					//				}
					//return res;
					break;
				case COUNT:
					//ExprTree tc= x.expr;
					//List<Datum[]> lc = ExpressionTree.doExpression(tc);
					//List<Datum[]>res_c=new ArrayList<Datum[]>();
					Iterator<Datum[]> ic = l.iterator();
					int count=0;
					while(ic.hasNext()){
						ic.next();
						count++;
					}
					Datum sendcount = null;
					sendcount = new Datum.Int(count);
					res[i++]=sendcount;
					//return res_c;
					break;
				case AVG:
					//ExprTree ta = x.expr;
					//List<Datum[]> la = ExpressionTree.doExpression(ta);
					//List<Datum[]>res_a=new ArrayList<Datum[]>();
					Iterator<Datum[]> ia = l.iterator();
					int sumi=0,counter=0;
					float sumf=0;
					float average=0;
					if(l.get(0)[0].getType() == Schema.Type.INT){
						while(ia.hasNext()){	
							sumi+=ia.next()[0].toInt();
							counter++;
						}
						average = (float)sumi/(float)counter;
					}
					if(l.get(0)[0].getType() == Schema.Type.FLOAT){
						while(ia.hasNext()){
							sumf+=ia.next()[0].toFloat();
							counter++;
						}
						average = sumf/counter;			
					}
					Datum sendavg = null;
					sendavg = new Datum.Flt(average);
					res[i++]=sendavg;
					//return res_a;
					break;
				case MAX:
					//ExprTree tmax = x.expr;
					//List<Datum[]> lmax = ExpressionTree.doExpression(tmax);
					//List<Datum[]>res_max = new ArrayList<Datum[]>();
					Iterator<Datum[]> imax = l.iterator();
					int maxi=0;
					float maxf=0;
					if(l.get(0)[0].getType() == Schema.Type.INT){
						List<Integer> a= new ArrayList<Integer>();
						//int j=0;
						while(imax.hasNext()){
							a.add(imax.next()[0].toInt());
						}
						maxi = maxInt(a);
						Datum sendmax = null;
						sendmax=new Datum.Int(maxi);
						res[i++]=sendmax;
					}
					if(l.get(0)[0].getType() == Schema.Type.FLOAT){
						List<Float> a = new ArrayList<Float>();
						//int j = 0;
						while(imax.hasNext()){
							a.add(imax.next()[0].toFloat());
						}
						maxf = maxFloat(a);
						Datum sendmax = null;
						sendmax=new Datum.Flt(maxf);
						res[i++]=sendmax;
					}
					//return res_max;
					break;
				case MIN:
					//ExprTree tmin = x.expr;
					//List<Datum[]> lmin = ExpressionTree.doExpression(tmin);
					//List<Datum[]>res_min = new ArrayList<Datum[]>();
					Iterator<Datum[]> imin = l.iterator();
					int mini=0;
					float minf=0;
					if(l.get(0)[0].getType() == Schema.Type.INT){
						List<Integer> a= new ArrayList<Integer>();
						//int j=0;
						while(imin.hasNext()){
							a.add(imin.next()[0].toInt());
						}
						mini = minInt(a);
						Datum sendmin = null;
						sendmin=new Datum.Int(mini);
						res[i++]=sendmin;
					}
					if(l.get(0)[0].getType() == Schema.Type.FLOAT){
						List<Float> a= new ArrayList<Float>();
						//int j = 0;
						while(imin.hasNext()){
							a.add(imin.next()[0].toFloat());
						}
						minf = minFloat(a);
						Datum sendmin = null;
						sendmin=new Datum.Flt(minf);
						res[i++]=sendmin;
					}
					//return res_min;
					break;
				default:
					return null;

				}
			}
			lsdatum.add(res);
			
		}
		return lsdatum;
	}
	public static int maxInt(List<Integer> t) {
		Iterator<Integer> itr = t.iterator(); // start with the first value\
		int maximum = itr.next();
		while(itr.hasNext()) {
			int item = itr.next();
			if (item > maximum) {
				maximum = item; // new maximum
			}
		}
		return maximum;
	}//end method max
	public static float maxFloat(List<Float> t) {
		Iterator<Float> itr = t.iterator(); // start with the first value\
		float maximum = itr.next();
		while(itr.hasNext()) {
			float item = itr.next();
			if (item > maximum) {
				maximum = item; // new maximum
			}
		}
		return maximum;
	}	
	public static int minInt(List<Integer> t) {
		Iterator<Integer> itr = t.iterator(); // start with the first value\
		int minimum = itr.next();
		while(itr.hasNext()) {
			int item = itr.next();
			if (item < minimum) {
				minimum = item; // new maximum
			}
		}
		return minimum;
	}	
	public static float minFloat(List<Float> t) {
		Iterator<Float> itr = t.iterator(); // start with the first value\
		float minimum = itr.next();
		while(itr.hasNext()) {
			float item = itr.next();
			if (item < minimum) {
				minimum = item; // new maximum
			}
		}
		return minimum;
	}
	
	public List findcolumns(){
		Iterator<AggColumn> it = a.aggregates.iterator();	//Iterator on aggregates
		AggColumn x;
		List<Column> arlsGroup=a.groupByVars;
		Iterator<Column> itGroup=arlsGroup.iterator();
		Column g;
		Iterator lsit=null;
		List ls=null;
		if(!(a.getChild().type.name().equals(PlanNode.Type.JOIN.toString()))&&!(a.getChild().type.name().equals(PlanNode.Type.SELECT.toString()))&&!(a.getChild().type.name().equals(PlanNode.Type.AGGREGATE.toString()))){
			ls=new ArrayList();
		}
		else{
		ls=Utility.findCol(a.getChild());
		}
		while(itGroup.hasNext()){
			g=itGroup.next();
			ExprTree e=g.expr;
			List ls2=new Expression(e).findColumns();
			lsit=ls2.iterator();
			//while(lsit.hasNext()){
//				ExprTree.VarLeaf vf=(VarLeaf) lsit.next();
//				//if(!ls.contains(vf.name)){
//					ls.add(vf);
//				//}
				ls.addAll(ls2);
			//}
		}
		
		
		while(it.hasNext()){
			x=it.next();
			ExprTree t= x.expr;
			
			List ls1=new Expression(t).findColumns();
			lsit=ls1.iterator();
//			while(lsit.hasNext()){
//				ExprTree.VarLeaf vf=(VarLeaf) lsit.next();
//				//if(!ls.contains(vf.name)){
//					ls.add(vf);
//				//}
//			}
			ls.addAll(ls1);

		}
		return ls;
		
	}


}
