package edu.buffalo.cse.sql.plan;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Table;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.Bool;
import edu.buffalo.cse.sql.data.Datum.Flt;
import edu.buffalo.cse.sql.data.Datum.Int;
import edu.buffalo.cse.sql.data.Datum.Str;

public class Scan extends ScanNode {

	public Scan(ScanNode objScan) {

		super(objScan.table,objScan.schema);
		
	}

	public List<Datum[]> doScan(){
	List<Datum[]> lsDatum=Sql.lsMapGlobalData.get(table);
	return lsDatum;

	}
}
