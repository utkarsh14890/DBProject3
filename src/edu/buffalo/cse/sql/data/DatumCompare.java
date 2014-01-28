package edu.buffalo.cse.sql.data;

import java.util.Comparator;

public class DatumCompare implements Comparator<Datum[]> {

	public DatumCompare() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(Datum[] arg0, Datum[] arg1) {
		 return Datum.compareRows(arg0, arg1);
	
	}

}
