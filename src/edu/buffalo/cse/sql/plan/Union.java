package edu.buffalo.cse.sql.plan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.util.Utility;

public class Union extends UnionNode{
	public UnionNode unode;
	public Union(UnionNode unode){
		this.unode=unode;
	}
	
	public List<Datum[]> doUnion() throws CastError{
		PlanNode LHSnode=unode.getLHS();
		PlanNode RHSnode=unode.getRHS();
		List<Datum[]> lsLHSDatum=Utility.switchNodes(LHSnode);
		List<Datum[]> lsRHSDatum=Utility.switchNodes(RHSnode);
		List<Datum[]> finalDatum= new ArrayList<Datum[]>();
		for( int i=0;i<lsLHSDatum.size();i++){
			finalDatum.add(lsLHSDatum.get(i));
			//finalDatum.add(lsRHSDatum.get(i));
		}
		for(int i=0;i<lsRHSDatum.size();i++){
			finalDatum.add(lsRHSDatum.get(i));
		}
		return finalDatum;
	}
	public List findcolumns(){
		List ls=new ArrayList();
		List lslhs=new ArrayList();
		List lsrhs=new ArrayList();
		lslhs=Utility.findCol(unode.getLHS());
		lsrhs=Utility.findCol(unode.getLHS());
		Iterator itlhs= lslhs.iterator();
		Iterator itrhs=lsrhs.iterator();
		while(itlhs.hasNext()){
			ls.add(itlhs.next());
		}
		while(itrhs.hasNext()){
			ls.add(itrhs.next());
		}
		return ls;
	}
	
}
