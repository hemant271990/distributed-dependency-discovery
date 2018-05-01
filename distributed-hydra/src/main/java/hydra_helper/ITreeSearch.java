package hydra_helper;

import java.util.Collection;
import java.util.function.Consumer;

//import ch.javasoft.bitset.IBitSet;
import hydra_helper.*;

public interface ITreeSearch {

	boolean add(IBitSet bs);

	void forEachSuperSet(IBitSet bitset, Consumer<IBitSet> consumer);

	void forEach(Consumer<IBitSet> consumer);

	void remove(IBitSet remove);

	boolean containsSubset(IBitSet bitset);

	Collection<IBitSet> getAndRemoveGeneralizations(IBitSet invalidDC);

}