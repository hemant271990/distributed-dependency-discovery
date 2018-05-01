package hydra_helper;

import java.util.Set;
import java.util.function.Consumer;

//import ch.javasoft.bitset.IBitSet;
import hydra_helper.*;

public interface ISubsetBackend {

	boolean add(IBitSet bs);

	Set<IBitSet> getAndRemoveGeneralizations(IBitSet invalidFD);

	boolean containsSubset(IBitSet add);

	void forEach(Consumer<IBitSet> consumer);

}
