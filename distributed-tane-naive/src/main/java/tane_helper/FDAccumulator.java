package tane_helper;
import java.util.*;
import org.apache.spark.AccumulatorParam;

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
public class FDAccumulator implements AccumulatorParam<List<HashMap<String, HashSet<String>>>> {

    @Override
    public List<HashMap<String, HashSet<String>>> zero(List<HashMap<String, HashSet<String>>> initialValue) {
        if(initialValue == null) initialValue = new ArrayList<HashMap<String, HashSet<String>>>();
        return initialValue;
    }

    @Override
    public List<HashMap<String, HashSet<String>>> addInPlace(List<HashMap<String, HashSet<String>>> map1, List<HashMap<String, HashSet<String>>> map2) {
    
		for(HashMap<String, HashSet<String>> hm : map2)
			map1.add(hm);
  
        return map1;
    }

    @Override
    public List<HashMap<String, HashSet<String>>> addAccumulator(List<HashMap<String, HashSet<String>>> map1, List<HashMap<String, HashSet<String>>> map2) {

    	for(HashMap<String, HashSet<String>> hm : map2)
            map1.add(hm);

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
