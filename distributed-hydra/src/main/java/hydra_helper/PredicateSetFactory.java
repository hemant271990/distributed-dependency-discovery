package hydra_helper;

import hydra_helper.*;

public class PredicateSetFactory {

	public static PredicateBitSet create(Predicate... predicates) {
		PredicateBitSet set = new PredicateBitSet();
		for (Predicate p : predicates)
			set.add(p);
		return set;
	}
	
	public static PredicateBitSet create(IBitSet bitset) {
		return new PredicateBitSet(bitset);
	}

	public static PredicateBitSet create(PredicateBitSet pS) {
		return new PredicateBitSet(pS);
	}
}
