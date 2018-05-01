package fastfd_helper;

import fastfd_helper.AgreeSet;
import fastfd_helper.BitSetUtil;

import java.io.Serializable;

import org.apache.lucene.util.OpenBitSet;

public class DifferenceSet extends AgreeSet implements Serializable{

    public DifferenceSet(OpenBitSet obs) {

        this.attributes = obs;
    }

    public DifferenceSet() {

        this(new OpenBitSet());
    }

    @Override
    public String toString_() {

        return BitSetUtil.convertToIntList(this.attributes).toString() + "";
    }
}
