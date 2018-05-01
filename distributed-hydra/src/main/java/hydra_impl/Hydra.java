package hydra_impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*import de.hpi.naumann.dc.cover.PrefixMinimalCoverSearch;
import de.hpi.naumann.dc.denialcontraints.DenialConstraintSet;
import de.hpi.naumann.dc.evidenceset.IEvidenceSet;
import de.hpi.naumann.dc.evidenceset.build.Operator;
import de.hpi.naumann.dc.evidenceset.HashEvidenceSet;
import de.hpi.naumann.dc.evidenceset.IEvidenceSet;
import de.hpi.naumann.dc.evidenceset.TroveEvidenceSet;
import de.hpi.naumann.dc.evidenceset.build.sampling.ColumnAwareEvidenceSetBuilder;
import de.hpi.naumann.dc.evidenceset.build.sampling.OrderedCluster;
import de.hpi.naumann.dc.evidenceset.build.sampling.OrderedCluster;
import de.hpi.naumann.dc.evidenceset.build.sampling.SystematicLinearEvidenceSetBuilder;
import de.hpi.naumann.dc.evidenceset.build.sampling.WeightedRandomPicker;
import de.hpi.naumann.dc.evidenceset.build.sampling.ColumnAwareEvidenceSetBuilder.ColumnData;
import de.hpi.naumann.dc.evidenceset.build.sampling.ColumnAwareEvidenceSetBuilder.SamplingMethod;
import de.hpi.naumann.dc.evidenceset.build.sampling.ColumnAwareEvidenceSetBuilder.SamplingType;
import de.hpi.naumann.dc.input.ColumnPair;
import de.hpi.naumann.dc.input.Input;
import de.hpi.naumann.dc.input.ParsedColumn;
import de.hpi.naumann.dc.predicates.Predicate;
import de.hpi.naumann.dc.predicates.PredicateBuilder;
import de.hpi.naumann.dc.predicates.PredicateProvider;
import de.hpi.naumann.dc.predicates.operands.ColumnOperand;
import de.hpi.naumann.dc.predicates.sets.PredicateBitSet;
import de.hpi.naumann.dc.predicates.sets.PredicateSetFactory;*/
import gnu.trove.iterator.TIntIterator;
import hydra_helper.*;

public class Hydra {

	protected int sampleRounds = 20;
	protected double efficiencyThreshold = 0.005d;
	protected PredicateBuilder predicates;
	protected Map<ColumnPair, PredicateBitSet[]> map;
	public Dataset<Row> df = null;
	private Boolean noCrossColumn = true;
	private double minimumSharedValue = 0.15d;
	Input input;
	PredicateBuilder predicates2;
	public JavaSparkContext sc;
	public static int numPartitions = 55;
	public static int samplingCount = 1;
	
	public DenialConstraintSet execute() {
		initialize();
		
		System.out.println("Building approximate evidence set...");
		//IEvidenceSet sampleEvidenceSet = new SystematicLinearEvidenceSetBuilder(predicates,sampleRounds).buildEvidenceSet(input);
		long t1 = System.currentTimeMillis();
		IEvidenceSet sampleEvidenceSet = new SystematicLinearEvidenceSetBuilder2(predicates,sampleRounds, sc).buildEvidenceSet(input);
		
		System.out.println("Estimation size systematic sampling:" + sampleEvidenceSet.size());
		long t2 = System.currentTimeMillis();
		System.out.println(" --Linear evidence time "+ (t2-t1)/1000);
		
		HashEvidenceSet set = new HashEvidenceSet();
		sampleEvidenceSet.getSetOfPredicateSets().forEach(i -> set.add(i));
		//IEvidenceSet fullEvidenceSet = new ColumnAwareEvidenceSetBuilder2(predicates, sc).buildEvidenceSet(set, input, efficiencyThreshold);
		//IEvidenceSet fullEvidenceSet = new ColumnAwareEvidenceSetBuilder(predicates).buildEvidenceSet(set, input, efficiencyThreshold);
		ColumnAwareEvidenceSetBuilder2.samplingCount = samplingCount;
		ColumnAwareEvidenceSetBuilder2.batchSize = numPartitions;
		ColumnAwareEvidenceSetBuilder2.numPartitions = numPartitions;
		IEvidenceSet fullEvidenceSet = new ColumnAwareEvidenceSetBuilder2(predicates, sc).buildEvidenceSetUniformPartition(set, input, efficiencyThreshold, df);
		
		System.out.println("Evidence set size deterministic sampler: " + fullEvidenceSet.size());
		long t3 = System.currentTimeMillis();
		System.out.println(" --Column aware evidence time "+ (t3-t2)/1000);
		
		/*DenialConstraintSet dcsApprox = new PrefixMinimalCoverSearch(predicates).getDenialConstraints(fullEvidenceSet);*/
		DenialConstraintSet dcsApprox = new PrefixMinimalCoverSearch(predicates).getDenialConstraints(fullEvidenceSet);
		System.out.println("DC count approx:" + dcsApprox.size());
		long t4 = System.currentTimeMillis();
		System.out.println(" --Approx DC time "+ (t4-t3)/1000);
		
		dcsApprox.minimize();
		System.out.println("DC count approx after minimize:" + dcsApprox.size());
		long t5 = System.currentTimeMillis();
		System.out.println(" --Approx DC minimize time "+ (t5-t4)/1000);
		
		IEvidenceSet result = new ResultCompletion2(input, predicates, sc).complete(dcsApprox, sampleEvidenceSet,
				fullEvidenceSet);
		long t6 = System.currentTimeMillis();
		System.out.println(" --Evidence set completion time "+ (t6-t5)/1000);
		
		DenialConstraintSet dcs = new PrefixMinimalCoverSearch(predicates).getDenialConstraints(result);
		long t7 = System.currentTimeMillis();
		System.out.println(" --getDenialConstraints time "+ (t7-t6)/1000);
		
		dcs.minimize();
		long t8 = System.currentTimeMillis();
		System.out.println(" --DC minimize time "+ (t8-t7)/1000);
		
		//for(DenialConstraint c : dcs)
		//	System.out.println(c.toResult());
		return dcs;
	}
	
	public void initialize() {
		input = new Input(df);
		System.out.println("Column count: "+input.getColumnCount());
		System.out.println("Row count: "+input.getLineCount());
		predicates = new PredicateBuilder(input, noCrossColumn, minimumSharedValue);
		//for(Predicate p : predicates.getPredicates())
		//	System.out.println(p);
		System.out.println("Predicate space size:" + predicates.getPredicates().size());
	}
		
	private static Logger log = LoggerFactory.getLogger(Hydra.class);

}
