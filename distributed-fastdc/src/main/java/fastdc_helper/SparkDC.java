package fastdc_helper;

import java.util.List;

/**
 * Created by Jian on 2017-04-25.
 */
public class SparkDC {
    public List<SparkPredicate> predicates;
    public int numTuples;

    public SparkDC() {}

    public SparkDC(List<SparkPredicate> predicates, int numTuples) {
        this.predicates = predicates;
        this.numTuples = numTuples;
    }

    @Override
    public String toString() {
        String ret = "DC: not (\n";
        for (SparkPredicate predicate : predicates)
            ret += predicate.toString(numTuples) + "\n";
        ret += ")";
        return ret;
    }
}
