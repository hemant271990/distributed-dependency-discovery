package hyucc_helper;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.OpenBitSet;

public class ValidationResult {
	public int validations = 0;
	public int intersections = 0;
	public List<OpenBitSet> invalidUCCs = new ArrayList<>();
	public List<IntegerPair> comparisonSuggestions = new ArrayList<>();
	public void add(ValidationResult other) {
		this.validations += other.validations;
		this.intersections += other.intersections;
		this.invalidUCCs.addAll(other.invalidUCCs);
		this.comparisonSuggestions.addAll(other.comparisonSuggestions);
	}
	
}