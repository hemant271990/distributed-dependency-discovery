package hyfd_helper;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.OpenBitSet;

public class ValidationResult {
	public int validations = 0;
	public int intersections = 0;
	public List<FD> invalidFDs = new ArrayList<>();
	public List<IntegerPair> comparisonSuggestions = new ArrayList<>();
	public void add(ValidationResult other) {
		this.validations += other.validations;
		this.intersections += other.intersections;
		this.invalidFDs.addAll(other.invalidFDs);
		this.comparisonSuggestions.addAll(other.comparisonSuggestions);
	}
	
}