package edu.buffalo.cse.sql.plan;

import java.util.List;

import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.util.ManageList;

public class Null extends NullSourceNode{
	NullSourceNode node;
	public Null(NullSourceNode node){
		super(node.rows);
		this.node=node;
		
	}
	
	public List<Datum[]> doNull(){
		Datum[] arr=new Datum[node.rows];
		for(Datum d:arr){
			d=null;
		}
		return new ManageList().toListOfDatumArray(arr);
	}

}
