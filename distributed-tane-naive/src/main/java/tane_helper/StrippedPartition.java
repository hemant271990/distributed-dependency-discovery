package tane_helper;
import java.io.Serializable;

import java.util.*;

// import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
// import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
// import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;


// import it.unimi.dsi.fastutil;

public class StrippedPartition implements Serializable{
    private double error;
    private long elementCount;
    private List<List<Integer>> strippedPartition = null;

    public StrippedPartition() {}
    /**
     * Create a StrippedPartition with only one equivalence class with the definied number of elements. <br/>
     * Tuple ids start with 0 to numberOfElements-1
     *
     * @param numberTuples
     */
    public StrippedPartition(Integer numberTuples) {
        this.strippedPartition = new ArrayList<List<Integer>>();
        this.elementCount = numberTuples;
        // StrippedPartition only contains partition with more than one elements.
        if (numberTuples > 1) {
            List<Integer> newEqClass = new ArrayList<Integer>();
            for (int i = 0; i < numberTuples; i++) {
                newEqClass.add(i);
            }
            this.strippedPartition.add(newEqClass);
        }
        this.calculateError();
    }
    public StrippedPartition(double err, long ec) {
        this.error = err;
        this.elementCount = ec;
    }
    /**
     * Create a StrippedPartition from a HashMap mapping the values to the tuple ids.
     *
     * @param partition
     */
    public StrippedPartition(Hashtable<Integer, List<Integer>> partition) {
        this.strippedPartition = new ArrayList<List<Integer>>();
        this.elementCount = 0;

        //create stripped partitions -> only use equivalence classes with size > 1.
        for (List<Integer> eqClass : partition.values()) {
            if (eqClass.size() > 1) {
                strippedPartition.add(eqClass);
                elementCount += eqClass.size();
            }
        }
        this.calculateError();
    }

    public StrippedPartition(List<List<Integer>> sp, long elementCount) {
        this.strippedPartition = sp;
        this.elementCount = elementCount;
        this.calculateError();

    }

    public StrippedPartition(StrippedPartition other) {
        this.error = other.error;
        this.elementCount = other.elementCount;
    }

    public double getError() {
        return error;
    }
    
    public long getElementCount() {
        return elementCount;
    }

    public List<List<Integer>> getStrippedPartition() {
        return this.strippedPartition;
    }

    private void calculateError() {
        // calculating the error. Dividing by the number of entries
        // in the whole population is not necessary.
        this.error = this.elementCount - this.strippedPartition.size();
    }

    public void empty() {
        this.strippedPartition = new ArrayList<List<Integer>>();
        this.elementCount = 0;
        this.error = 0.0;
    }
}

