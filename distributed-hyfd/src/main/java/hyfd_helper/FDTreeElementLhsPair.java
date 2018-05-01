package hyfd_helper;

import org.apache.lucene.util.OpenBitSet;

public class FDTreeElementLhsPair {
	
	private final FDTreeElement element;
	private final OpenBitSet lhs;
	
	public FDTreeElement getElement() {
		return this.element;
	}

	public OpenBitSet getLhs() {
		return this.lhs;
	}

	public FDTreeElementLhsPair(FDTreeElement element, OpenBitSet lhs) {
		this.element = element;
		this.lhs = lhs;
	}
}

