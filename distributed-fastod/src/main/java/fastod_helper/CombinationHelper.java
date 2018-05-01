package fastod_helper;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.lucene.util.OpenBitSet;

import java.io.Serializable;

public class CombinationHelper implements Serializable {

    private static final long serialVersionUID = 1L;

    private OpenBitSet rhsCandidates;
    private boolean valid;

    private StrippedPartition partition;

    private double error; 		// keep error and element count in combinationHelper itself 
    private long elementCount;	// so that there is no need to load the whole strippedPartition.
    private long uniqueCount;
    
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
        partition = null;
    }

    //OD
    private ObjectArrayList<OpenBitSet> swapCandidates;

    //OD
    public ObjectArrayList<OpenBitSet> getSwapCandidates() {
        return swapCandidates;
    }

    //OD
    public void setSwapCandidates(ObjectArrayList<OpenBitSet> swapCandidates) {
        this.swapCandidates = swapCandidates;
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

	public long getUniqueCount() {
		return uniqueCount;
	}

	public void setUniqueCount(long uniqueCount) {
		this.uniqueCount = uniqueCount;
	}

}

