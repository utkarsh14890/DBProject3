
package edu.buffalo.cse.sql.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.buffalo.cse.sql.Schema;
import edu.buffalo.cse.sql.SqlException;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.data.Datum.CastError;
import edu.buffalo.cse.sql.data.DatumBuffer;
import edu.buffalo.cse.sql.data.DatumSerialization;
import edu.buffalo.cse.sql.data.InsufficientSpaceException;
import edu.buffalo.cse.sql.buffer.BufferManager;
import edu.buffalo.cse.sql.buffer.BufferException;
import edu.buffalo.cse.sql.buffer.ManagedFile;
import edu.buffalo.cse.sql.buffer.FileManager;
import edu.buffalo.cse.sql.test.TestDataStream;
import edu.buffalo.cse.sql.util.ManageList;

public class ISAMIndex implements IndexFile {
	public static int pageNo=0;
	ManagedFile file;
	IndexKeySpec keySpec;
	public ISAMIndex(ManagedFile file, IndexKeySpec keySpec)
			throws IOException, SqlException
			{
		this.file=file;
		this.keySpec=keySpec;
			}

	public static ISAMIndex create(FileManager fm,
			File path,
			Iterator<Datum[]> dataSource,
			IndexKeySpec key)
					throws SqlException, IOException
					{
		ManagedFile mf=fm.open(path);
		ByteBuffer byteBuffer=null;
		TestDataStream data=((TestDataStream)dataSource);
		DatumBuffer datumBuffer=null;
		LinkedHashMap<Integer,Datum[]> hmp= new LinkedHashMap<Integer,Datum[]>();
		//logic for leaves
		mf.resize(1);
		while(data.hasNext()){
			Datum[] row = dataSource.next();
			if(hmp.get(pageNo)==null){

				byteBuffer= mf.safePin(pageNo);
				datumBuffer = new DatumBuffer(byteBuffer,data.getSchema());
				hmp.put(pageNo, row);
				datumBuffer.initialize(4);
				DatumSerialization.write(byteBuffer, 0, new Datum.Int(-1));
			}
			else{ 
				if(DatumSerialization.getLength(row)>datumBuffer.remaining()-8){
					mf.unpin(pageNo,true);
					pageNo=pageNo+1;
					byteBuffer= mf.safePin(pageNo);
					datumBuffer = new DatumBuffer(byteBuffer,data.getSchema());
					hmp.put(pageNo, row);
					datumBuffer.initialize(4);
					DatumSerialization.write(byteBuffer, 0, new Datum.Int(-1));
				} 
			}
			datumBuffer.write(row); 	 
			//			System.out.print("LeafPage: "+pageNo+" value:");
			//			for(Datum d:row){
			//				System.out.print(" "+d.toInt());
			//			}
			//			System.out.println();
		}//end of logic for leaves
		mf.unpin(pageNo,true);
		BuildIndex(hmp,key,fm,mf,data);
		mf.flush();
		return null;
		//throw new SqlException("Unimplemented");
					}

	public static void BuildIndex(LinkedHashMap hmp,IndexKeySpec key,FileManager fm,ManagedFile mf,TestDataStream data) throws BufferException, IOException, InsufficientSpaceException, CastError{
		Iterator it=hmp.entrySet().iterator();
		//Iterator it1=hmp.entrySet().iterator();
		//int pageNo=0;
		pageNo=pageNo+1;
		//	System.out.println("******* hmp *******");
		//		while(it1.hasNext()){
		//			Map.Entry<Integer,Datum[]> pairs1=(Map.Entry)it1.next();
		//			int keyyy=pairs1.getKey();
		//			Datum[] row1=(Datum[]) pairs1.getValue();
		//		//	Datum[] indexKey1=key.createKey(row1);
		//			//System.out.println("Page No :"+pairs1.getKey()+" Row:"+key.createKey(pairs1.getValue())[0]);
		//			System.out.print("pageNo: "+keyyy+" Row:");
		//			if(pageNo==3226){
		//				System.out.print("");
		//			}
		//			for(Datum d:row1){
		//				System.out.print(" "+d.toInt());
		//			}
		//			System.out.println();
		//		}
		LinkedHashMap hmp1= new LinkedHashMap();
		ByteBuffer byteBuffer=null;
		DatumBuffer datumBuffer=null;
		while(it.hasNext()){
			Map.Entry pairs=(Map.Entry)it.next();
			int Leafpage=(Integer) pairs.getKey();
			Datum[] row=(Datum[]) pairs.getValue();
			Datum[] indexKey=key.createKey(row);
			if(hmp1.get(pageNo)==null){
				byteBuffer=mf.safePin(pageNo);
				datumBuffer = new DatumBuffer(byteBuffer,data.getSchema());
				hmp1.put(pageNo, row);
				datumBuffer.initialize(4);
				DatumSerialization.write(byteBuffer, 0, new Datum.Int(0));
				Datum[] val=new Datum[1];
				val[0]=new Datum.Int(Leafpage);
				datumBuffer.write(val);
				//	System.out.println("Val:"+val[0].toInt());
			}
			else{
				if(DatumSerialization.getLength(row)+4>datumBuffer.remaining()-8){
					mf.unpin(pageNo,true);
					pageNo=pageNo+1;
					byteBuffer= mf.safePin(pageNo);  
					datumBuffer = new DatumBuffer(byteBuffer,data.getSchema());
					hmp1.put(pageNo, row);	  
					datumBuffer.initialize(4);
					DatumSerialization.write(byteBuffer, 0, new Datum.Int(0));
					Datum[] val=new Datum[1];
					val[0]=new Datum.Int(Leafpage);
					datumBuffer.write(val);
					//	System.out.println("Val:"+val[0].toInt());
				} 
				else{
					datumBuffer.write(indexKey);
					//System.out.print("Key:");
					//					for(Datum d:indexKey){
					//						System.out.print(" "+d.toInt());
					//					}
					//					System.out.println();
					//System.out.print("Key:"+indexKey.toInt());
					Datum[] val=new Datum[1];
					val[0]=new Datum.Int(Leafpage);
					datumBuffer.write(val);
					//System.out.println("Val:"+val[0].toInt());
				}
			}
			//datumBuffer.write(indexKey);

		}
		mf.unpin(pageNo,true);
		if(hmp1.size()>1){
			//			Iterator it2=hmp1.entrySet().iterator();
			//			//System.out.println("******* hmp1 *******");
			//			while(it2.hasNext()){
			//				Map.Entry<Integer,Datum[]> pairs2=(Map.Entry)it2.next();
			//				int key1=pairs2.getKey();
			//				//System.out.print("Key: "+key1+" Value:");
			//				Datum[] row2=(Datum[]) pairs2.getValue();
			//				//Datum[] indexKey2=key.createKey(row2);
			//				for(Datum d:row2){
			//					//System.out.print(" "+d.toInt());
			//				}
			//				//System.out.println();
			//				//System.out.println("Page No :"+pairs2.getKey()+" Row:"+key.createKey(pairs2.getValue())[0]);
			//			}
			hmp=hmp1;
			BuildIndex(hmp,key,fm,mf,data);
		}

		byteBuffer=mf.safePin(pageNo);
		// datumBuffer = new DatumBuffer(byteBuffer,data.getSchema());
		DatumSerialization.write(byteBuffer, 0, new Datum.Int(1));
		mf.unpin(pageNo);
		//return null;

	}
	public IndexIterator scan() 
			throws SqlException, IOException
			{	
		int value=0;
		int pageNo=file.size()-1;
		int root=0;
		int pointer=0;
		int nextpointer=0;
		ByteBuffer byteBuffer=null;
		DatumBuffer dataBuffer=null;
		while(pageNo!=-1){
			byteBuffer=file.safePin(pageNo);
			value= DatumSerialization.read(byteBuffer,0,Schema.Type.INT).toInt();
			if(value==1)
				break;
			file.unpin(pageNo);
			pageNo--;	
		}
		root=pageNo;
		byteBuffer=file.safePin(root);
		dataBuffer=new DatumBuffer(byteBuffer,keySpec.rowSchema());
		pointer= DatumSerialization.read(byteBuffer,4,Schema.Type.INT).toInt();
		int prev=0;
		prev=root;
		file.unpin(root);

		while(true){
			byteBuffer=file.safePin(pointer);
			value=DatumSerialization.read(byteBuffer,0,Schema.Type.INT).toInt();
			if(value==-1){
				file.unpin(pointer);
				break;
			}
			nextpointer= DatumSerialization.read(byteBuffer,4,Schema.Type.INT).toInt();
			file.unpin(pointer);
			prev=pointer;
			pointer=nextpointer;
		}

		DatumStreamIterator index=new DatumStreamIterator(file,keySpec.rowSchema());
		index.currPage(pointer);
		index.maxPage(prev-1);

		ArrayList arr= new ArrayList();
		index.currRecord(0);
		byteBuffer=file.safePin(prev-1);
		dataBuffer = new DatumBuffer(byteBuffer,keySpec.rowSchema());
		Datum[] maxRecord=dataBuffer.read(dataBuffer.length()-1);
		index.maxRecord(maxRecord);
		//index.key(keySpec);
		index.ready();
		file.unpin(prev-1);
		return index;

		//got the first leaf page

		//return null;
		//throw new SqlException("Unimplemented");

			}

	public IndexIterator rangeScanTo(Datum[] toKey)throws SqlException, IOException
	{
		int value=0;
		int pageNo=file.size()-1;
		int root=0;
		int pointer=0;
		int nextpointer=0;
		ByteBuffer byteBuffer=null;
		DatumBuffer dataBuffer=null;
		while(pageNo!=-1){
			byteBuffer=file.safePin(pageNo);
			value= DatumSerialization.read(byteBuffer,0,Schema.Type.INT).toInt();
			if(value==1)
				break;
			file.unpin(pageNo);
			pageNo--;	
		}
		root=pageNo;
		pointer=root;
		while(true){

			byteBuffer= file.safePin(pointer);
			dataBuffer = new DatumBuffer(byteBuffer,keySpec.rowSchema());
			int value1=DatumSerialization.read(byteBuffer,0,Schema.Type.INT).toInt();
			if(value1==-1){
				file.unpin(pointer);
				break;
			}
			file.unpin(pointer);
			Datum[] row= null;
			ArrayList<Integer> arls= new ArrayList<Integer>();
			int j=4;
			Datum[]indexKey=null;
			while(j<=((((dataBuffer.length()/2)*keySpec.keySchema().length) +(dataBuffer.length()-dataBuffer.length()/2)) *4)-4){
				j=j+4;
				indexKey=DatumSerialization.read(byteBuffer,j,keySpec.keySchema());
				//		System.out.println(indexKey[0].toInt());
				int offset=keySpec.compare(indexKey, toKey);
				if(offset==1){
					pointer=DatumSerialization.read(byteBuffer,j-4,Schema.Type.INT).toInt();
					break;
				}
				else if(offset==-1|| offset==0){		
					j=j+(keySpec.keySchema().length)*4;
					pointer=DatumSerialization.read(byteBuffer,j,Schema.Type.INT).toInt();
				}
			}

		}
		DatumStreamIterator index=new DatumStreamIterator(file,keySpec.rowSchema());
		index.currPage(0);
		index.maxPage(pointer);
		index.currRecord(0);
		//index.key(keySpec);
		Datum[] maxrecord=null;
		byteBuffer=file.safePin(pointer);
		dataBuffer = new DatumBuffer(byteBuffer,keySpec.rowSchema());
		int offset=2;
		int first=0;
		for(int i=0;i<dataBuffer.length();i++){
			Datum[] row=dataBuffer.read(i);
			Datum[] key=keySpec.createKey(row);
			offset=keySpec.compare(key, toKey);
			if(offset==0 ){
				maxrecord=dataBuffer.read(i);
				first=i; //change for phase 3
				break;
			}
			else if(offset==1){
				maxrecord=dataBuffer.read(i-1);
				break;
			}

			else{
				maxrecord=dataBuffer.read(i);
			}

		}
		//changes for phase 3:starts
		if(offset==0){
			int max=maxpage(root);
			int i=0;
			while(pointer<=max){
				byteBuffer=file.safePin(pointer);
				dataBuffer = new DatumBuffer(byteBuffer,keySpec.rowSchema());
				for( i=first;i<dataBuffer.length();i++){
					Datum[] row=dataBuffer.read(i);
					Datum[] key=keySpec.createKey(row);
					offset=keySpec.compare(key, toKey);
					if(offset==0 ){
						maxrecord=dataBuffer.read(i);
					}
					else{
						break;
					}
				}
				if( i==dataBuffer.length() && offset==0){
					file.unpin(pointer);
					pointer=pointer+1;
					first=0;
				}
				else
				{	
					if( i==0)
						pointer=pointer-1;
					break;
				}
			}
		}
		index.maxPage(pointer);
		//changes for phase 3:ends
		index.maxRecord(maxrecord);	
		index.ready();
		return index;
	}

	public IndexIterator rangeScanFrom(Datum[] fromKey)throws SqlException, IOException
	{
		int value=0;
		int pageNo=file.size()-1;
		int root=0;
		int pointer=0;
		int nextpointer=0;
		ByteBuffer byteBuffer=null;
		DatumBuffer dataBuffer=null;
		DatumStreamIterator index=new DatumStreamIterator(file,keySpec.rowSchema());
		while(pageNo!=-1){
			byteBuffer=file.safePin(pageNo);
			value= DatumSerialization.read(byteBuffer,0,Schema.Type.INT).toInt();
			if(value==1)
				break;
			file.unpin(pageNo);
			pageNo--;	
		}
		root=pageNo;
		pointer=root;
		byteBuffer=file.safePin(root);
		dataBuffer=new DatumBuffer(byteBuffer,keySpec.rowSchema());
		pointer= DatumSerialization.read(byteBuffer,4,Schema.Type.INT).toInt();
		int prev=0;
		prev=root;
		file.unpin(root);

		while(true){
			byteBuffer=file.safePin(pointer);
			value=DatumSerialization.read(byteBuffer,0,Schema.Type.INT).toInt();
			if(value==-1){
				file.unpin(pointer);
				break;
			}
			nextpointer= DatumSerialization.read(byteBuffer,4,Schema.Type.INT).toInt();
			file.unpin(pointer);
			prev=pointer;
			pointer=nextpointer;
		}

		index.maxPage(prev-1);
		byteBuffer=file.safePin(prev-1);
		dataBuffer = new DatumBuffer(byteBuffer,keySpec.rowSchema());
		Datum[] maxRecord=dataBuffer.read(dataBuffer.length()-1);
		index.maxRecord(maxRecord);
		file.unpin(prev-1);
		//from logic
		pointer=root;

		while(true){
			byteBuffer= file.safePin(pointer);
			dataBuffer = new DatumBuffer(byteBuffer,keySpec.rowSchema());
			int value1=DatumSerialization.read(byteBuffer,0,Schema.Type.INT).toInt();
			if(value1==-1){
				file.unpin(pointer);
				break;
			}
			file.unpin(pointer);
			int j=4;
			Datum[]indexKey=null;
			while(j<=((((dataBuffer.length()/2)*keySpec.keySchema().length) +(dataBuffer.length()-dataBuffer.length()/2)) *4)-4){
				j=j+4;
				indexKey=DatumSerialization.read(byteBuffer,j,keySpec.keySchema());
				//	System.out.println(indexKey[0].toInt());
				int offset=keySpec.compare(indexKey, fromKey);
				if(offset==1){
					pointer=DatumSerialization.read(byteBuffer,j-4,Schema.Type.INT).toInt();
					break;
				}
				else if(offset==-1|| offset==0){		
					j=j+(keySpec.keySchema().length)*4;
					pointer=DatumSerialization.read(byteBuffer,j,Schema.Type.INT).toInt();
				}
			}
		}

		int currecord=0;
		int currpage=pointer;
		byteBuffer=file.safePin(pointer);
		dataBuffer = new DatumBuffer(byteBuffer,keySpec.rowSchema());
		int offset=2;
		for(int i=0;i<dataBuffer.length();i++){
			Datum[] row=dataBuffer.read(i);
			Datum[] key=keySpec.createKey(row);
			offset=keySpec.compare(key, fromKey);
			if(offset==0 ){
				currecord=i;
				break;
			}
			else if(offset==1){
				currecord=i;
				break;
			}

			else{
				if(i==dataBuffer.length()-1){
					if(pointer!=prev-1){
						currpage=pointer+1;
						currecord=0;
					}
					else
						currecord=i;
				}
				else
					currecord=i;
			}

		}
		file.unpin(pointer);
		index.currPage(currpage);
		index.currRecord(currecord);
		index.ready();
		return index;

	}

	public IndexIterator rangeScan(Datum[] start, Datum[] end)throws SqlException, IOException
	{

		DatumStreamIterator index=new DatumStreamIterator(file,keySpec.rowSchema());
		ISAMIndex isam=new ISAMIndex(file, keySpec);

		DatumStreamIterator to=(DatumStreamIterator)isam.rangeScanTo(end);

		DatumStreamIterator from=(DatumStreamIterator)isam.rangeScanFrom(start);

		index.currPage(from.currPage);
		index.currRecord(from.currRecord);
		index.maxPage(to.maxPage);
		index.maxRecord(to.maxRecord);

		index.ready();

		return index;

	}

	public List<Datum[]> get(Datum[] key) throws SqlException, IOException
	{
		int value=0;
		int pageNo=file.size()-1;
		int root=0;
		int pointer=0;
		int nextpointer=0;
		ByteBuffer byteBuffer=null;
		DatumBuffer dataBuffer=null;
		while(pageNo!=-1){
			byteBuffer=file.safePin(pageNo);
			value= DatumSerialization.read(byteBuffer,0,Schema.Type.INT).toInt();
			if(value==1)
				break;
			file.unpin(pageNo);
			pageNo--;	
		}
		root=pageNo;
		pointer=root;
		while(true){
			byteBuffer= file.safePin(pointer);
			dataBuffer = new DatumBuffer(byteBuffer,keySpec.rowSchema());
			int value1=DatumSerialization.read(byteBuffer,0,Schema.Type.INT).toInt();
			if(value1==-1){
				file.unpin(pointer);
				break;
			}
			file.unpin(pointer);
			Datum[] row= null;
			ArrayList<Integer> arls= new ArrayList<Integer>();
			int j=4;
			Datum[]indexKey=null;
			while(j<=((((dataBuffer.length()/2)*keySpec.keySchema().length) +(dataBuffer.length()-dataBuffer.length()/2)) *4)-4){
				j=j+4;
				indexKey=DatumSerialization.read(byteBuffer,j,keySpec.keySchema());
				//	System.out.println(indexKey[0].toInt());
				int offset=keySpec.compare(indexKey, key);
				if(offset==1){
					pointer=DatumSerialization.read(byteBuffer,j-4,Schema.Type.INT).toInt();
					break;
				}
				else if(offset==-1|| offset==0){		
					j=j+(keySpec.keySchema().length)*4;
					pointer=DatumSerialization.read(byteBuffer,j,Schema.Type.INT).toInt();
				}
			}
		}


		Datum[] finalRow=null;
		byteBuffer=file.safePin(pointer);
		dataBuffer = new DatumBuffer(byteBuffer,keySpec.rowSchema());
		int offset=2;

		for(int i=0;i<dataBuffer.length();i++){
			Datum[] row=dataBuffer.read(i);
			Datum[] rowkey=keySpec.createKey(row);
			offset=keySpec.compare(rowkey, key);
			if(offset==0 ){
				finalRow=dataBuffer.read(i);
				break;
			}
		}
		file.unpin(pointer);
		return new ManageList().toListOfDatumArray(finalRow);

	}

	public int maxpage(int pointer) throws BufferException, IOException, CastError{
		ByteBuffer byteBuffer=null;
		DatumBuffer dataBuffer=null;
		int value=0;
		int nextpointer=0;
		int prev=0;
		while(true){
			byteBuffer=file.safePin(pointer);
			value=DatumSerialization.read(byteBuffer,0,Schema.Type.INT).toInt();
			if(value==-1){
				file.unpin(pointer);
				break;
			}
			nextpointer= DatumSerialization.read(byteBuffer,4,Schema.Type.INT).toInt();
			file.unpin(pointer);
			prev=pointer;
			pointer=nextpointer;
		}
		return prev-1;
	}


}