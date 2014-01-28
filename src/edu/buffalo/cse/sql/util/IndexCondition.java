package edu.buffalo.cse.sql.util;

import edu.buffalo.cse.sql.Index.IndexType;
import edu.buffalo.cse.sql.data.Datum;
import edu.buffalo.cse.sql.plan.ExprTree;

public class IndexCondition {

	int index;
	

	public int getIndex() {
		return index;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public ExprTree.OpCode getOpCode() {
		return opCode;
	}

	public void setOpCode(ExprTree.OpCode opCode) {
		this.opCode = opCode;
	}

	public Datum getValue() {
		return value;
	}

	public void setValue(Datum d) {
		this.value = d;
	}

	ExprTree.OpCode opCode;
	Datum value;
	
	public IndexCondition(int index,ExprTree.OpCode op,Datum d) {
		this.value=d;
		this.opCode=op;
		this.index=index;
	}

}
