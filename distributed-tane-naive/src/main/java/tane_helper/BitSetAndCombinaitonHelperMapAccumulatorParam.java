package tane_helper;

import org.apache.spark.AccumulatorParam;

import java.util.BitSet;
import java.util.Hashtable;

import tane_helper.CombinationHelper;

/**
 *
 * This is an accumulator for a map of properties and their BitSet.
 * Each Key in the map is a property. The value of this property key is a BitSet.
 * Each bit in the BitSet represent a unique Subject (Resource).
 * Hence, the size of the BitSet is the size of unique subjects in all triples.
 * If a bit is set to 1, it means that it has a value for this property.
 *
 * This complicated accumulator will help us scan the data only once to find combinations
 * of all interesting properties.
 *
 * @author Mina Farid
 * @author Jian Li
 */
public class BitSetAndCombinaitonHelperMapAccumulatorParam implements AccumulatorParam<Hashtable<BitSet, CombinationHelper>> {

    @Override
    public Hashtable<BitSet, CombinationHelper> zero(Hashtable<BitSet, CombinationHelper> initialValue) {
        if(initialValue == null) initialValue = new Hashtable<BitSet, CombinationHelper>();
        return initialValue;
    }

    @Override
    public Hashtable<BitSet, CombinationHelper> addInPlace(Hashtable<BitSet, CombinationHelper> map1, Hashtable<BitSet, CombinationHelper> map2) {
        for (BitSet property : map2.keySet()) {
            if(!map1.containsKey(property)) {
                map1.put(property, map2.get(property));
            }
        }

        return map1;
    }

    @Override
    public Hashtable<BitSet, CombinationHelper> addAccumulator(Hashtable<BitSet, CombinationHelper> map1, Hashtable<BitSet, CombinationHelper> map2) {
        for (BitSet property : map2.keySet()) {
            if(!map1.containsKey(property)) {
                map1.put(property, map2.get(property));
            }
        }

        return map1;
    }
}

/*
    class BitSetAccumulatorParam implements AccumulatorParam<BitSet> {

        @Override
        public BitSet zero(BitSet initialValue) {
            return new BitSet(initialValue.length());
        }

        @Override
        public BitSet addInPlace(BitSet set1, BitSet set2) {
            set1.or(set2);
            return set1;
        }
    } */
