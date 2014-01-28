
package edu.buffalo.cse.sql.index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

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

public class HashIndex implements IndexFile {
	//public static final int pageSize = 1024;
	static int countPagesUsed;
	static HashMap<Integer,DatumBuffer> hashMap=new HashMap<Integer, DatumBuffer>();
	public ManagedFile mf;
	public IndexKeySpec keySpec;
	public HashIndex(ManagedFile file, IndexKeySpec keySpec)
			throws IOException, SqlException
			{
		//throw new SqlException("Unimplemented");
		this.mf = file;
		this.keySpec = keySpec;
			}

	public static HashIndex create(FileManager fm,
			File path,
			Iterator<Datum[]> dataSource,
			IndexKeySpec key,
			int directorySize)
					throws SqlException, IOException
					{
		ManagedFile mf_create=fm.open(path);
		mf_create.resize(directorySize);
		ByteBuffer byteBuffer;
		countPagesUsed=directorySize;
		TestDataStream data=((TestDataStream)dataSource);
		DatumBuffer writer;
		Schema.Type schema[]=data.getSchema();

	//	HashMap<Integer,DatumBuffer> hashMap=new HashMap<Integer, DatumBuffer>();
		while(dataSource.hasNext()){

			Datum[] row = dataSource.next();
			int bucketNumber = key.hashRow(row) % directorySize;

			int freePage= getFreePage(bucketNumber,mf_create);
			byteBuffer = mf_create.safePin(freePage);
			
//			if(freePage==20)
//			{
//			//	System.out.println(freePage);
//			}

			if(hashMap.containsKey(freePage)){
			//	writer=hashMap.get(freePage); // wrong value due to mutable and immutable objects.
				// the writer has a byte Buffer but it is a stale byte buffer .. as the value of byteBuffer has been
				// changed. Thus , the byte buffer is not valid. Solved by creatig a new writer everytime with the new 
				// Byte Buffer.
				writer=new DatumBuffer(byteBuffer, schema);
			}
			else{
				writer=new DatumBuffer(byteBuffer,schema);
				writer.initialize(8);
				DatumSerialization.write(byteBuffer, 4, new Datum.Int(directorySize));
				DatumSerialization.write(byteBuffer, 0, new Datum.Int(-1));
				hashMap.put(freePage, writer);
			}

			if(DatumSerialization.getLength(row) > writer.remaining()-8){
				DatumSerialization.write(byteBuffer, 0, new Datum.Int(countPagesUsed));
				ByteBuffer buffer= mf_create.safePin(countPagesUsed);

				writer=new DatumBuffer(buffer,schema);
				writer.initialize(8);
				DatumSerialization.write(buffer, 4, new Datum.Int(directorySize));
				DatumSerialization.write(buffer, 0, new Datum.Int(-1));
				hashMap.put((countPagesUsed), writer);

				writer.write(row);
				
//				System.out.print("bucketID: "+countPagesUsed+" Row:");
//				for(Datum d:row){
//					System.out.print(" "+d.toInt());
//				}
				
				//System.out.println();
				mf_create.unpin(countPagesUsed,true);
				mf_create.unpin(freePage, true);

				countPagesUsed++;
			}
			else{
				writer.write(row);
				
//				System.out.print("bucketID: "+freePage+" Row:");
//				for(Datum d:row){
//					System.out.print(" "+d.toInt());
//				}
			//	System.out.println();
				
				mf_create.unpin(freePage, true);
			}
		}
		mf_create.flush();
		return null;
					}

	private static int getFreePage(int bucketNumber, ManagedFile mf_get) throws BufferException, IOException, CastError {
		ByteBuffer buffer= mf_get.safePin(bucketNumber);
		int tempPage=bucketNumber;
		while((DatumSerialization.read(buffer, 0, Schema.Type.INT).toInt()!=-1) && (hashMap.containsKey(tempPage))){
			int ijk=tempPage;
			tempPage=DatumSerialization.read(buffer, 0, Schema.Type.INT).toInt();
			mf_get.unpin(ijk);	// here or before above Line
			buffer=mf_get.safePin(tempPage);
		}
		mf_get.unpin(tempPage);
		return tempPage;
	}
	public IndexIterator scan() 
			throws SqlException, IOException
			{
		throw new SqlException("Unimplemented");
			}

	public IndexIterator rangeScanTo(Datum[] toKey)
			throws SqlException, IOException
			{
		throw new SqlException("Unimplemented");
			}

	public IndexIterator rangeScanFrom(Datum[] fromKey)
			throws SqlException, IOException
			{
		throw new SqlException("Unimplemented");
			}

	public IndexIterator rangeScan(Datum[] start, Datum[] end)
			throws SqlException, IOException
			{
		throw new SqlException("Unimplemented");
			}

	public List<Datum[]> get(Datum[] key) throws SqlException, IOException
	{
		ArrayList<Datum[]> finalDatum = new ArrayList<Datum[]>();
		//throw new SqlException("Unimplemented");
		int d = 0;
		if(key==null){
			System.out.println("No key passed");
		}
		if(mf.size()!=0){
			ByteBuffer b = mf.pin(0);
			d = DatumSerialization.read(b,4,Schema.Type.INT).toInt();
			mf.unpin(0);
		}
		int bucket = keySpec.hashKey(key)%d;
		ByteBuffer buffer= mf.pin(bucket);
		DatumBuffer db = new DatumBuffer(buffer,keySpec.rowSchema());
		int pos;
		pos = db.find(key);
		Datum res[] = db.read(pos);
		int tempPage=bucket;
		while(keySpec.compare(keySpec.createKey(res), key)!=0 && !(DatumSerialization.read(buffer, 0, Schema.Type.INT).equals(new Datum.Int(-1)))){
			mf.unpin(tempPage);	// here or after next Line
			tempPage=DatumSerialization.read(buffer, 0, Schema.Type.INT).toInt();
			buffer=mf.pin(tempPage);
			db = new DatumBuffer(buffer,keySpec.rowSchema());
			pos = db.find(key);
			res = db.read(pos);
		}
		if(keySpec.compare(keySpec.createKey(res), key)==0){
			while(!(DatumSerialization.read(buffer, 0, Schema.Type.INT).equals(new Datum.Int(-1)))){
					while(!(pos>db.length())){
						res = db.read(pos);
						if(keySpec.compare(keySpec.createKey(res), key)==0){
							finalDatum.add(res);
							pos+=1;
						}
						else
							return finalDatum;
					}
					mf.unpin(tempPage);
					tempPage = DatumSerialization.read(buffer, 0, Schema.Type.INT).toInt();
					buffer = mf.pin(tempPage);
					db = new DatumBuffer(buffer,keySpec.rowSchema());
					pos = db.find(key);
			}
			while(!(pos>db.length())){
				res = db.read(pos);
				if(keySpec.compare(keySpec.createKey(res), key)==0){
					finalDatum.add(res);
					pos+=1;
				}
				else
					break;
			}
			mf.unpin(tempPage);
			return finalDatum;
		}
		else{
			mf.unpin(tempPage);
			//System.out.println("Key Not Found");
			return null;
		}
	}	
}
//	public Datum[] get(Datum[] key) throws SqlException, IOException
//	{
//
//		Datum [] outRow=null;
//		int directorySize = 0;
//		int pos;
//		
//		if(key==null){
//			System.out.println("No key passed");
//			return null;
//		}
//		
//		if(key[0].toInt()==1793){
//			System.out.println(key[0].toInt());
//		}
//
//		
//
//		if(mf.size()>0){
//			ByteBuffer b = mf.safePin(0);
//			directorySize = DatumSerialization.read(b,4,Schema.Type.INT).toInt();
//			mf.unpin(0);
//		}
//		else{
//			return null;
//		}
//
//		int bucket = keySpec.hashKey(key)% directorySize;
//
//
//		ByteBuffer buffer;
//		DatumBuffer db;
//
//		int temp=bucket;
//		Datum[] tempRes=null;
//
//		do{
//			buffer=mf.safePin(temp);
//			db = new DatumBuffer(buffer,keySpec.rowSchema());
//			//buffer.asIntBuffer().array();
//			pos=db.find(key);
//			tempRes=db.read(pos);
//			if(keySpec.compare(keySpec.createKey(tempRes), key)==0){
//				outRow=tempRes;
//				mf.unpin(temp);
//				break;
//			}
//			else{
//				mf.unpin(temp);
//				temp=DatumSerialization.read(buffer, 0, Schema.Type.INT).toInt();
//				System.out.println(temp);
//			}
//		}while(temp!=-1);
//
//
//		return outRow;
//		}
//
//
