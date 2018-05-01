package tane_helper;

import java.io.Serializable;
import java.util.BitSet;

import tane_helper.CombinationHelper;

public final class Package implements Serializable {
	public Integer prefix_index;
	public BitSet bitset;
	public CombinationHelper ch;

	public Package() {}
	public Package(Integer index, BitSet b, CombinationHelper c) {
        prefix_index = index;
        bitset = new BitSet();
        bitset = (BitSet) b.clone();
        ch = c;
    }

	// public Package(BitSet b, CombinationHelper c) {
 //        bitset = b;
 //        ch = c;
 //    }

}
