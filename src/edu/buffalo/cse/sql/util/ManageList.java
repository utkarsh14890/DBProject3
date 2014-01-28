package edu.buffalo.cse.sql.util;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


/*To call the methods do the following thing :

1. Create Object
ManageList m=new ManageList("pass the list of Datum[] here");
ManageList m= new ManageList();   //**** this will be used to convert Datum[] to List of Datum[]

2.	Call method
Datum[] col=m.getColumn("pass the index of the column");
Datum[] ro=m.getRow("pass the number of desired row");
Datum datu=m.getDatum("pass the no.of row and index of the column");

 ***** List<Datum[]> listOfDatumArry=m.toListOfDatumArray("pass the Datum[] in this"); 

 */


import edu.buffalo.cse.sql.data.Datum;
public class ManageList {

	List<Datum[]> data;

	public ManageList(List<Datum[]> data){
		this.data=data;

	}

	public ManageList(){
		data=null;
	}

	public Datum[] getColumn(int column){
		Datum[] ret=new Datum[data.size()];

		Iterator<Datum[]> it_data=data.iterator();
		int i=0;
		while(it_data.hasNext()){
			ret[i++]=it_data.next()[column];
		}

		return ret;
	}

	public Datum[] getRow(int row){
		return data.get(row);
	}

	public Datum getDatum(int row, int column){
		return (data.get(row))[column];
	}

	public List<Datum[]> toListOfDatumArray(Datum[] arr){
		ArrayList<Datum[]> ret=new ArrayList<Datum[]>();
		Datum[] a;
		for(Datum d :arr){
			a=new Datum[1];
			a[0]=d;
			ret.add(a);
		}

		return ret;
	}

}
