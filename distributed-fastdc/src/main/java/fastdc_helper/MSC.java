package fastdc_helper;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/**
 * Created by jian on 4/13/17.
 */
public class MSC {
    public BitSet cover;
    public List<MSC> sources;
    public boolean hasGeneral;
    public boolean mustMinimal;

    public MSC(BitSet cover) {
        this.cover = cover;
        this.sources = new ArrayList<MSC>();
    }

    public MSC(int size) {
        this.cover = new BitSet(size);
        this.sources = new ArrayList<MSC>();
    }

    public MSC(MSC other) {
        cover = (BitSet)other.cover.clone();
        sources = other.sources;
        hasGeneral = other.hasGeneral;
        mustMinimal = other.mustMinimal;
    }

    public static BitSet inheritMSC(MSC source, int size, List<Integer> predicateSpaceMapping) {
        BitSet cover = new BitSet(size);
        int numOverlapPredicates = 0;
        for (int k = 0; k < predicateSpaceMapping.size(); ++ k) {
            int x = predicateSpaceMapping.get(k);
            if (source.cover.get(x)) {
                ++ numOverlapPredicates;
                cover.set(k);
            }
        }
        if (numOverlapPredicates != source.cover.cardinality()) return null;
        return cover;
    }

    public void addSource(MSC source) {
        sources.add(source);
    }

    public String toString() {
        //System.out.println("Minimal Set Covers:");
        return cover.toString();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MSC msc = (MSC) o;

        return cover.equals(msc.cover);
    }

    @Override
    public int hashCode() {
        return cover.hashCode();
    }
}
