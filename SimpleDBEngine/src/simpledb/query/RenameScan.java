package simpledb.query;

import simpledb.record.RID;

public class RenameScan implements UpdateScan { //I implemented the UpdateScan interface, in case the set... methods is needed to
    private Scan s;
    private String oldFldname;
    private String newFldname;
	
	public RenameScan(Scan s, String oldFldname, String newFldname){
		this.s = s;
		this.oldFldname = oldFldname;
		this.newFldname = newFldname;
	}
	
	@Override
	public void beforeFirst() {
		s.beforeFirst();
		
	}

	@Override
	public boolean next() {
		return s.next();
	}

	@Override
	public int getInt(String fldname) {
        String actualFldname = actualFieldName(fldname);
        return s.getInt(actualFldname);
	}

	@Override
	public String getString(String fldname) {
        String actualFldname = actualFieldName(fldname);
        return s.getString(actualFldname);
	}

	@Override
	public Constant getVal(String fldname) {
        String actualFldname = actualFieldName(fldname);
        return s.getVal(actualFldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return s.hasField(fldname) || fldname.equals(oldFldname);
	}

	@Override
	public void close() {
		s.close();
	}

	@Override
	public void setVal(String fldname, Constant val) {
	    UpdateScan us = (UpdateScan) s;
	    String actualFldname = actualFieldName(fldname);
        us.setVal(actualFldname, val);
	}

	@Override
	public void setInt(String fldname, int val) {
		UpdateScan us = (UpdateScan) s;
		String actualFldname = actualFieldName(fldname);
        us.setInt(actualFldname, val);
	}

	@Override
	public void setString(String fldname, String val) {
		UpdateScan us = (UpdateScan) s;
		String actualFldname = actualFieldName(fldname);
        us.setString(actualFldname, val);
	}

	@Override
	public void insert() {
		UpdateScan us = (UpdateScan) s;
		us.insert();
	}

	@Override
	public void delete() {
		UpdateScan us = (UpdateScan) s;
		us.delete();
	}

	@Override
	public RID getRid() {
		UpdateScan us = (UpdateScan) s;
		return us.getRid();
	}

	@Override
	public void moveToRid(RID rid) {
		UpdateScan us = (UpdateScan) s;
		us.moveToRid(rid);
	}
	
    private String actualFieldName(String fldname) { //I created this private  method to determine the actual field name in the underlying scan based on the given field name.
        if (fldname.equals(newFldname))
            return oldFldname;
        return fldname;
    }

}
