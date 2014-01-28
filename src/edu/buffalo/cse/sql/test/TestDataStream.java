/**
 * A testing harness that generates arbitrary length data streams.  Thanks to a
 * hardcoded seed value, the data streams for one set of parameters (#keys, ...)
 * will be random, but identical each time.
 * 
 * You can vary the # of keys, the # of non-key values, and the the # of rows
 * generated.  Each data column will be an integer.  Keys are guaranteed to be
 * generated in monotonically increasing order, with keys on the left taking 
 * priority (in other words, the generated dataset is already sorted).
 * 
 * You can fine-tune the behavior of the key-incrementing process.  There is a
 * chaos parameter that controls how rapidly key-steps occur.  The higher this
 * value is, the more different adjacent keys will be.
 * 
 * The guaranteeKeyStep parameter, if true, will ensure that no two data rows
 * will have the same key.  If false, two data rows may have the same key -- 
 * especially for low values of the chaos parameter.
 *
 * TestDataStream also includes a pair of validation methods.  These allow you
 * to verify the accuracy of an index scan iterator that you provide.  Entries
 * in the provided iterator will be read off, invalid entries will be reported
 * as an error, and the method will return false
 **/

package edu.buffalo.cse.sql.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.Schema.Column;
import edu.buffalo.cse.sql.Schema.TableFromFile;
import edu.buffalo.cse.sql.Sql;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.Bool;
import edu.buffalo.cse.sql.data.Datum.Flt;
import edu.buffalo.cse.sql.data.Datum.Str;
import edu.buffalo.cse.sql.data.DatumCompare;

public class TestDataStream implements Iterator<Datum[]> {

	int rows;
	int values;
	int[] curr;
	int keyCols;
	int chaos;
	TableFromFile tableFromFile;
	
	public Map<Datum, ArrayList<Datum[]>> tree_lsDatum= new TreeMap<Datum, ArrayList<Datum[]>>();
	Iterator<Entry<Datum, ArrayList<Datum[]>>> it_tree_lsDatum;

	public TestDataStream(int keys, int values, int rows)
	{ 
		this(null,keys,0, values, rows, keys*10, true);
	}

	public TestDataStream(TableFromFile tableFromFile,int noOfKeys, int keyCols,int values, int rows, int chaos, 
			boolean guaranteeKeyStep)
	{
		this.tableFromFile= tableFromFile;
		this.keyCols=keyCols;
		if(this.tableFromFile!=null && guaranteeKeyStep==false){
			try {
				readTableFromFile();
				Set<Entry<Datum, ArrayList<Datum[]>>> set=tree_lsDatum.entrySet();
				it_tree_lsDatum=set.iterator();
			//	it_lsDatum=lsDatum.iterator();
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		this.rows = tree_lsDatum.size();
	//	this.values = tree_lsDatum.size() > 0 ? tree_lsDatum.get(0).length-noOfKeys : 0;
		this.curr = new int[noOfKeys];
		this.chaos = chaos;
	}

	public Schema.Type[] getSchema()
	{
		return tableFromFile.getSchema();
	}

	public boolean hasNext()
	{
		return it_tree_lsDatum.hasNext();
		//return tree_lsDatum.hasNext();
	}

	public Datum[] next()
	{
		return it_tree_lsDatum.next().getValue().get(0);
	}

	public int getRowCount(){
		return tree_lsDatum.size();
	}

	public void remove()
	{
		throw new UnsupportedOperationException();
	}

	private void readTableFromFile() throws NumberFormatException, IOException {


		BufferedReader bufferedReader = new BufferedReader(new FileReader(tableFromFile.getFile()));
		String data;
		while ((data = bufferedReader.readLine()) != null){ 
			String arrtoken[];

			if(Sql.flag_TPCH==0){
				arrtoken=data.split(",");
			}
			else{
				arrtoken=data.split("\\|");
			}

			Datum[] arrdatum=new Datum[arrtoken.length];
			for(int i=0;i<arrdatum.length;i++){ 

				Datum datum=null;
				String token=arrtoken[i];
				Schema.Column col=tableFromFile.get(i);

				switch(col.type){

				case INT:
					try {
						datum= new Datum.Int(Integer.parseInt(token));
					} catch (NumberFormatException e) {
						if(token.contains("#")){
							System.out.println("contain #");
							token=token.substring(token.indexOf("#")+1);
							datum= new Datum.Int(Integer.parseInt(token));
						}
						else if(token.contains("-")){
							SimpleDateFormat df=new SimpleDateFormat("yyyy-mm-dd");
							df.setLenient(false);
							try {
								df.parse(token);
								token=token.replace("-","");
								datum= new Datum.Int(Integer.parseInt(token));
							} catch (java.text.ParseException e1) {
								System.out.println("Contains \"-\" but not a date");
							}
						}
						else
							e.printStackTrace();
					}
					break;
				case FLOAT:
					datum= new Flt(Float.parseFloat(token));
					break;
				case BOOL:
					if(token.equals("True"))
						datum= Bool.TRUE;
					else
						datum=Bool.FALSE;
					break;
				case STRING:
					datum= new Str(token);
					break;
				default:
					break;
				}
				arrdatum[i]=datum;
			}
			
//Datum key[]=new Datum[this.keyCols.length];
//			
//			int j=0;
//			for(int i:this.keyCols)
//				key[j++]=arrdatum[i];
//			
			Datum key=arrdatum[keyCols];
			if(tree_lsDatum.containsKey(key)){
				ArrayList<Datum[]> arr=tree_lsDatum.get(key);
				arr.add(arrdatum);
				tree_lsDatum.put(key, arr);
			}else{
				ArrayList<Datum[]> arr=new ArrayList<Datum[]>();
				arr.add(arrdatum);
				tree_lsDatum.put(key, arr);
				//tree_lsDatum.put(key, new ArrayList<Datum[]>().add(arrdatum));	
			}
			
			//tree_lsDatum.put(arrdatum[keyCols[0]], arrdatum);
		//	lsDatum.add(arrdatum);
		}
		bufferedReader.close();
	}
}