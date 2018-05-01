package tane_helper;

import java.io.Serializable;
//import org.apache.spark.util.collection.BitSet;
import java.util.BitSet;

import tane_helper.StrippedPartition;

public class CombinationHelper implements Serializable {

    private static final long serialVersionUID = 1L;

    private BitSet rhsCandidates;
    private boolean valid;
    private double error; 		// keep error and element count in combinationHelper itself 
    private long elementCount;	// so that there is no need to load the whole strippedPartition.
    private StrippedPartition partition;

    public CombinationHelper() {
        valid = true;
    }

    public CombinationHelper(CombinationHelper other) {
        this.rhsCandidates = other.rhsCandidates;
        this.valid = other.valid;
        //this.partition = new StrippedPartition(other.getPartition().error, other.getPartition().elementCount);
    }

    public BitSet getRhsCandidates() {
        return rhsCandidates;
    }

    public void setRhsCandidates(BitSet rhsCandidates) {
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
        partition = null;
    }

	public double getError() {
		return error;
	}

	public void setError(double error) {
		this.error = error;
	}

	public long getElementCount() {
		return elementCount;
	}

	public void setElementCount(long elementCount) {
		this.elementCount = elementCount;
	}

}

