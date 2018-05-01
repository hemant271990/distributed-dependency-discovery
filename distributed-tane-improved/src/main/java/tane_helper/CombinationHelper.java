package tane_helper;

import org.apache.lucene.util.OpenBitSet;

import java.io.Serializable;

public class CombinationHelper implements Serializable {

    private static final long serialVersionUID = 1L;

    private OpenBitSet rhsCandidates;
    private double error; 		// keep error and element count in combinationHelper itself 
    private long uniqueCount;	// so that there is no need to load the whole strippedPartition.
    private boolean valid;

    private StrippedPartition partition;

    public CombinationHelper() {
        valid = true;
    }

    public OpenBitSet getRhsCandidates() {
        return rhsCandidates;
    }

    public void setRhsCandidates(OpenBitSet rhsCandidates) {
        this.rhsCandidates = rhsCandidates;
    }

    public StrippedPartition getPartition() {
        return partition;
    }

    public void setPartition(StrippedPartition partition) {
        this.partition = partition;
    }

    public boolean isValid() {
        return valid;
    }

    public void setInvalid() {
        this.valid = false;
        //partition = null;
    }

	public long getUniqueCount() {
		return uniqueCount;
	}

	public void setUniqueCount(long uniqueCount) {
		this.uniqueCount = uniqueCount;
	}

	public double getError() {
		return error;
	}

	public void setError(double error) {
		this.error = error;
	}

	

}
