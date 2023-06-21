package simpledb.query;

public class UnionScan implements Scan{
	private Scan s1, s2;
    private boolean s1HasNext, s2HasNext; //I want to check if the corresponding methods have more records to retrieve
										  //I will use them to get.. methods.
    									  //In the methods, I will first check if a field exist in scan1, if not the code will look to scan2.
	public UnionScan(Scan s1, Scan s2) {
		this.s1 = s1;
		this.s2 = s2;
        this.s1HasNext = s1.next();
        this.s2HasNext = s2.next();
	}

	@Override
	public void beforeFirst() {
		s1.beforeFirst();
		s2.beforeFirst();
        s1HasNext = s1.next();
        s2HasNext = s2.next();
	}

	@Override
	public boolean next() {
       if (s1HasNext) {
            if (s1.next()) {
                return true;
            } else {
                s1HasNext = false;
            }
        }
        if (s2HasNext) {
            if (s2.next()) {
                return true;
            } else {
                s2HasNext = false;
            }
        }
        return false;
	}

	@Override
	public int getInt(String fldname) {
        if (s1HasNext) {
            try {
                return s1.getInt(fldname);
            } catch (RuntimeException e) {
            }
        }
        if (s2HasNext) {// Like I mentioned, if the field is not found in s1, the code will try s2
            return s2.getInt(fldname);
        }
        throw new RuntimeException("Field not found: " + fldname);
	}

	@Override
	public String getString(String fldname) {
        if (s1HasNext) {
            try {
                return s1.getString(fldname);
            } catch (RuntimeException e) {
                
            }
        }
        if (s2HasNext) { 
            return s2.getString(fldname);
        }
        throw new RuntimeException("Field not found: " + fldname);
	}

	@Override
	public Constant getVal(String fldname) {
        if (s1HasNext) {
            try {
                return s1.getVal(fldname);
            } catch (RuntimeException e) {
            }
        }
        if (s2HasNext) {
            return s2.getVal(fldname);
        }
        throw new RuntimeException("Field not found: " + fldname);
	}

	@Override
	public boolean hasField(String fldname) {
		return s1.hasField(fldname) || s2.hasField(fldname);
	}

	@Override
	public void close() {
		s1.close();
		s2.close();	
	}
	
	
}
