package hyucc_impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/*import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.algorithm_types.BooleanParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.IntegerParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.RelationalInputParameterAlgorithm;
import de.metanome.algorithm_integration.algorithm_types.UniqueColumnCombinationsAlgorithm;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirement;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementBoolean;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementInteger;
import de.metanome.algorithm_integration.configuration.ConfigurationRequirementRelationalInput;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import de.metanome.algorithms.hyucc.structures.IntegerPair;
import de.metanome.algorithms.hyucc.structures.PLIBuilder;
import de.metanome.algorithms.hyucc.structures.PositionListIndex;
import de.metanome.algorithms.hyucc.structures.UCCList;
import de.metanome.algorithms.hyucc.structures.UCCSet;
import de.metanome.algorithms.hyucc.structures.UCCTree;
import de.metanome.algorithms.hyucc.utils.Logger;
import de.metanome.algorithms.hyucc.utils.ValueComparator;
import de.uni_potsdam.hpi.utils.CollectionUtils;
import de.uni_potsdam.hpi.utils.FileUtils;*/
import hyucc_helper.*;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

public class HyUCC {
    
	public enum Identifier {
		INPUT_GENERATOR, NULL_EQUALS_NULL, VALIDATE_PARALLEL, ENABLE_MEMORY_GUARDIAN, MAX_UCC_SIZE, INPUT_ROW_LIMIT
	};

	/*private RelationalInputGenerator inputGenerator = null;
	private UniqueColumnCombinationResultReceiver resultReceiver = null;*/

	private static ValueComparator valueComparator;
	private static final MemoryGuardian memoryGuardian = new MemoryGuardian(true);
	
	private static boolean validateParallel = true;
	private static int maxUccSize = -1;
	private static int inputRowLimit = -1;				// Maximum number of rows to be read from for analysis; values smaller or equal 0 will cause the algorithm to read all rows
	
	private static float efficiencyThreshold = 0.01f;
	
	public static Dataset<Row> df = null;
	public static JavaSparkContext sc;
	public static String datasetFile = null;
	public static List<List<String>> tuples = null;
	public static String[] columnNames = null;
	public static long numberTuples = 0;
	public static Integer numberAttributes = 0;
	/*private static String tableName;
	private static List<String> attributeNames;
	private static int numAttributes;*/

	/*@Override
	public ArrayList<ConfigurationRequirement<?>> getConfigurationRequirements() {
		ArrayList<ConfigurationRequirement<?>> configs = new ArrayList<ConfigurationRequirement<?>>(5);
		configs.add(new ConfigurationRequirementRelationalInput(HyUCC.Identifier.INPUT_GENERATOR.name()));
		
		ConfigurationRequirementBoolean nullEqualsNull = new ConfigurationRequirementBoolean(HyUCC.Identifier.NULL_EQUALS_NULL.name());
		Boolean[] defaultNullEqualsNull = new Boolean[1];
		defaultNullEqualsNull[0] = new Boolean(true);
		nullEqualsNull.setDefaultValues(defaultNullEqualsNull);
		nullEqualsNull.setRequired(true);
		configs.add(nullEqualsNull);

		ConfigurationRequirementBoolean validateParallel = new ConfigurationRequirementBoolean(HyUCC.Identifier.VALIDATE_PARALLEL.name());
		Boolean[] defaultValidateParallel = new Boolean[1];
		defaultValidateParallel[0] = new Boolean(this.validateParallel);
		validateParallel.setDefaultValues(defaultValidateParallel);
		validateParallel.setRequired(true);
		configs.add(validateParallel);

		ConfigurationRequirementBoolean enableMemoryGuardian = new ConfigurationRequirementBoolean(HyUCC.Identifier.ENABLE_MEMORY_GUARDIAN.name());
		Boolean[] defaultEnableMemoryGuardian = new Boolean[1];
		defaultEnableMemoryGuardian[0] = new Boolean(this.memoryGuardian.isActive());
		enableMemoryGuardian.setDefaultValues(defaultEnableMemoryGuardian);
		enableMemoryGuardian.setRequired(true);
		configs.add(enableMemoryGuardian);
		
		ConfigurationRequirementInteger maxLhsSize = new ConfigurationRequirementInteger(HyUCC.Identifier.MAX_UCC_SIZE.name());
		Integer[] defaultMaxLhsSize = new Integer[1];
		defaultMaxLhsSize[0] = new Integer(this.maxUccSize);
		maxLhsSize.setDefaultValues(defaultMaxLhsSize);
		maxLhsSize.setRequired(false);
		configs.add(maxLhsSize);

		ConfigurationRequirementInteger inputRowLimit = new ConfigurationRequirementInteger(HyUCC.Identifier.INPUT_ROW_LIMIT.name());
		Integer[] defaultInputRowLimit = { Integer.valueOf(this.inputRowLimit) };
		inputRowLimit.setDefaultValues(defaultInputRowLimit);
		inputRowLimit.setRequired(false);
		configs.add(inputRowLimit);
		
		return configs;
	}

	@Override
	public void setResultReceiver(UniqueColumnCombinationResultReceiver resultReceiver) {
		this.resultReceiver = resultReceiver;
	}

	@Override
	public void setBooleanConfigurationValue(String identifier, Boolean... values) throws AlgorithmConfigurationException {
		if (HyUCC.Identifier.NULL_EQUALS_NULL.name().equals(identifier))
			this.valueComparator = new ValueComparator(values[0].booleanValue());
		else if (HyUCC.Identifier.VALIDATE_PARALLEL.name().equals(identifier))
			this.validateParallel = values[0].booleanValue();
		else if (HyUCC.Identifier.ENABLE_MEMORY_GUARDIAN.name().equals(identifier))
			this.memoryGuardian.setActive(values[0].booleanValue());
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}
	
	@Override
	public void setIntegerConfigurationValue(String identifier, Integer... values) throws AlgorithmConfigurationException {
		if (HyUCC.Identifier.MAX_UCC_SIZE.name().equals(identifier))
			this.maxUccSize = values[0].intValue();
		else if (HyUCC.Identifier.INPUT_ROW_LIMIT.name().equals(identifier))
			if (values.length > 0)
				this.inputRowLimit = values[0].intValue();
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}

	@Override
	public void setRelationalInputConfigurationValue(String identifier, RelationalInputGenerator... values) throws AlgorithmConfigurationException {
		if (HyUCC.Identifier.INPUT_GENERATOR.name().equals(identifier))
			this.inputGenerator = values[0];
		else
			this.handleUnknownConfiguration(identifier, CollectionUtils.concat(values, ","));
	}
	
	private void handleUnknownConfiguration(String identifier, String value) throws AlgorithmConfigurationException {
		throw new AlgorithmConfigurationException("Unknown configuration: " + identifier + " -> " + value);
	}*/

	/*private void initialize(RelationalInput relationalInput) throws AlgorithmExecutionException {
		this.tableName = relationalInput.relationName();
		this.attributeNames = relationalInput.columnNames();
		this.numAttributes = this.attributeNames.size();
		if (this.valueComparator == null)
			this.valueComparator = new ValueComparator(true);
	}*/

	public static void execute(){
		long startTime = System.currentTimeMillis();
		valueComparator = new ValueComparator(true);
		//this.executeFDEP();
		executeHyUCC();
		
		Logger.getInstance().writeln("Time: " + (System.currentTimeMillis() - startTime) + " ms");
	}

	private static void loadData() {
        tuples = new ArrayList<List<String>>();
        
        List<Row> table = df.collectAsList();
        //df.cache();
        numberTuples = df.count();

        int tupleId = 0;
        //Read and paste data into TranslationMaps
        while (tupleId < numberTuples) {
           Row row = table.get(tupleId);
           List<String> row_as_list = new ArrayList<String>();
           for (int i = 0; i < numberAttributes; i++) {
              //System.out.println(((Long)row.get(i)).intValue());

              String content = row.get(i).toString();
              row_as_list.add(content);
           }
           tuples.add(row_as_list);
           tupleId++;
           if(tupleId%100000 == 0) System.out.println("Copied tuple: " + tupleId);
        }
    }
	
	private static void executeHyUCC() {
		// Initialize
		Logger.getInstance().writeln("Initializing ...");
		/*RelationalInput relationalInput = this.getInput();
		this.initialize(relationalInput);*/
		loadData();
		
		///////////////////////////////////////////////////////
		// Build data structures for sampling and validation //
		///////////////////////////////////////////////////////
		
		// Calculate plis
		Logger.getInstance().writeln("Reading data and calculating plis ...");
		PLIBuilder pliBuilder = new PLIBuilder(numberTuples);
		List<PositionListIndex> plis = pliBuilder.getPLIs(tuples, numberAttributes, valueComparator.isNullEqualNull());

		final int numRecords = pliBuilder.getNumLastRecords();
		pliBuilder = null;
		
		/*if (numRecords == 0) {
			ObjectArrayList<ColumnIdentifier> columnIdentifiers = this.buildColumnIdentifiers();
			for (int attr = 0; attr < this.numAttributes; attr++)
				this.resultReceiver.receiveResult(new UniqueColumnCombination(new ColumnCombination(columnIdentifiers.get(attr))));
			return;
		}*/
		
		// Sort plis by number of clusters: For searching in the covers and for validation, it is good to have attributes with few non-unique values and many clusters left in the prefix tree
		Logger.getInstance().writeln("Sorting plis by number of clusters ...");
		Collections.sort(plis, new Comparator<PositionListIndex>() {
			@Override
			public int compare(PositionListIndex o1, PositionListIndex o2) {		
				int numClustersInO1 = numRecords - o1.getNumNonUniqueValues() + o1.getClusters().size();
				int numClustersInO2 = numRecords - o2.getNumNonUniqueValues() + o2.getClusters().size();
				return numClustersInO2 - numClustersInO1;
			}
		});
		
		// Calculate inverted plis
		Logger.getInstance().writeln("Inverting plis ...");
		int[][] invertedPlis = invertPlis(plis, numRecords);

		// Extract the integer representations of all records from the inverted plis
		Logger.getInstance().writeln("Extracting integer representations for the records ...");
		int[][] compressedRecords = new int[numRecords][];
		for (int recordId = 0; recordId < numRecords; recordId++)
			compressedRecords[recordId] = fetchRecordFrom(recordId, invertedPlis);
		invertedPlis = null;
		
		// Initialize the negative cover
		UCCSet negCover = new UCCSet(numberAttributes, maxUccSize);
		
		// Initialize the positive cover
		UCCTree posCover = new UCCTree(numberAttributes, maxUccSize);
		posCover.addMostGeneralUniques();
		
		//////////////////////////
		// Build the components //
		//////////////////////////

		// TODO: implement parallel sampling
		
		Sampler sampler = new Sampler(negCover, posCover, compressedRecords, plis, efficiencyThreshold, valueComparator, memoryGuardian);
		Inductor inductor = new Inductor(negCover, posCover, memoryGuardian);
		Validator validator = new Validator(negCover, posCover, compressedRecords, plis, efficiencyThreshold, validateParallel, memoryGuardian);
		
		List<IntegerPair> comparisonSuggestions = new ArrayList<>();
		do {
			UCCList newNonUCCs = sampler.enrichNegativeCover(comparisonSuggestions);
			inductor.updatePositiveCover(newNonUCCs);
			comparisonSuggestions = validator.validatePositiveCover();
		}
		while (comparisonSuggestions != null);
		negCover = null;
		
		// Output all valid FDs
		Logger.getInstance().writeln("Translating UCC-tree into result format ...");
		
		int numUCCs = posCover.addUniqueColumnCombinationsInto(buildColumnIdentifiers(), plis);
		
		Logger.getInstance().writeln("... done! (" + numUCCs + " UCCs)");
	}

	/*private static RelationalInput getInput() {
		RelationalInput relationalInput = this.inputGenerator.generateNewCopy();
		if (relationalInput == null)
			throw new InputGenerationException("Input generation failed!");
		return relationalInput;
	}*/
	
	private static ObjectArrayList<ColumnIdentifier> buildColumnIdentifiers() {
		ObjectArrayList<ColumnIdentifier> columnIdentifiers = new ObjectArrayList<ColumnIdentifier>(columnNames.length);
		for (String attributeName : columnNames)
			columnIdentifiers.add(new ColumnIdentifier(datasetFile, attributeName));
		return columnIdentifiers;
	}

	private static int[][] invertPlis(List<PositionListIndex> plis, int numRecords) {
		int[][] invertedPlis = new int[plis.size()][];
		for (int attr = 0; attr < plis.size(); attr++) {
			
			int[] invertedPli = new int[numRecords];
			Arrays.fill(invertedPli, -1);
			
			for (int clusterId = 0; clusterId < plis.get(attr).size(); clusterId++) {
				for (int recordId : plis.get(attr).getClusters().get(clusterId))
					invertedPli[recordId] = clusterId;
			}
			invertedPlis[attr] = invertedPli;
		}
		return invertedPlis;
	}
	
	private static int[] fetchRecordFrom(int recordId, int[][] invertedPlis) {
		int[] record = new int[numberAttributes];
		for (int i = 0; i < numberAttributes; i++)
			record[i] = invertedPlis[i][recordId];
		return record;
	}
}
