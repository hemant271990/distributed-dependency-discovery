package hydra_helper;

import java.util.Iterator;
import java.util.Set;

import hydra_helper.*;

public interface IEvidenceSet extends Iterable<PredicateBitSet> {

	boolean add(PredicateBitSet predicateSet);

	boolean add(PredicateBitSet create, long count);

	long getCount(PredicateBitSet predicateSet);

	Iterator<PredicateBitSet> iterator();

	Set<PredicateBitSet> getSetOfPredicateSets();

	int size();

	boolean isEmpty();

}