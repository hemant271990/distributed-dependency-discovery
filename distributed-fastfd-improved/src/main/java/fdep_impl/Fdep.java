package fdep_impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import fdep_helper.*;

public class Fdep implements Serializable{

	public static Dataset<Row> df = null;
	public static String[] columnNames = null;
	public static Integer numberTuples = 0;
	public static Integer numberAttributes = 0;
	public static JavaSparkContext sc;
	public static List<Integer> count_dependency;
	public static String datasetFile = null;
	public static int fdCount = 0;
	
    public static final String INPUT_SQL_CONNECTION = "DatabaseConnection";
    public static final String INPUT_TABLE_NAME = "Table_Name";
    public static final String INPUT_TAG = "Relational_Input";


    //private DatabaseConnectionGenerator databaseConnectionGenerator;
    //private RelationalInputGenerator relationalInputGenerator;
    //private static String tableName;
    //private List<String> columnNames;
    private static ArrayList<ColumnIdentifier> columnIdentifiers;

    //private int numberAttributes;

    private static FDTree negCoverTree;
    private static FDTree posCoverTree;
    private static ArrayList<List<String>> tuples;

    //private static FunctionalDependencyResultReceiver fdResultReceiver;

    public static void execute() {
    	long t1 = System.currentTimeMillis();
    	initialize();
        long t2 = System.currentTimeMillis();
        System.out.println(" initialize time(s): " + (t2-t1)/1000);
        
        negativeCover();
        long t3 = System.currentTimeMillis();
        System.out.println(" negativeCover time(s): " + (t3-t2)/1000);
        
        tuples = null;
        posCoverTree = new FDTree(numberAttributes);
        posCoverTree.addMostGeneralDependencies();
        OpenBitSet activePath = new OpenBitSet();
        
        long t4 = System.currentTimeMillis();
        calculatePositiveCover(negCoverTree, activePath);
        long t5 = System.currentTimeMillis();
        System.out.println(" calculatePositiveCover time(s): " + (t5-t4)/1000);
        System.out.println(" ===== TOTAL time(s): " + (t5-t1)/1000);
//		posCoverTree.filterGeneralizations();
        addAllDependenciesToResultReceiver();
        System.out.println(" ===== TOTAL FD Count: " + fdCount);
    }

    private static void initialize() {
        loadData();
        setColumnIdentifiers();
    }

    /**
     * Calculate a set of fds, which do not cover the invalid dependency lhs -> a.
     */
    private static void specializePositiveCover(OpenBitSet lhs, int a) {
        OpenBitSet specLhs = new OpenBitSet();

        while (posCoverTree.getGeneralizationAndDelete(lhs, a, 0, specLhs)) {
            for (int attr = numberAttributes; attr > 0; attr--) {
                if (!lhs.get(attr) && (attr != a)) {
                    specLhs.set(attr);
                    if (!posCoverTree.containsGeneralization(specLhs, a, 0)) {
                        posCoverTree.addFunctionalDependency(specLhs, a);
                    }
                    specLhs.clear(attr);
                }
            }
            specLhs = new OpenBitSet();
        }
    }

    private static void calculatePositiveCover(FDTreeElement negCoverSubtree, OpenBitSet activePath) {
        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (negCoverSubtree.isFd(attr - 1)) {
                specializePositiveCover(activePath, attr);
            }
        }

        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (negCoverSubtree.getChild(attr - 1) != null) {
                activePath.set(attr);
                calculatePositiveCover(negCoverSubtree.getChild(attr - 1), activePath);
                activePath.clear(attr);
            }
        }
    }


    /**
     * Calculate the negative Cover for the current relation.
     */
    private static void negativeCover() {
        negCoverTree = new FDTree(numberAttributes);
        for (int i = 0; i < tuples.size(); i++) {
            for (int j = i + 1; j < tuples.size(); j++) {
                violatedFds(tuples.get(i), tuples.get(j));
            }
        }
        negCoverTree.filterSpecializations();
        negCoverTree.printDependencies();
        
        /*Stack<FDTreeElement> st = new Stack<FDTreeElement>();
        st.push(negCoverTree);
        while(!st.isEmpty()) {
        	FDTreeElement e = st.pop();
        	if(e == null)
        		continue;
        	System.out.println(e.getRhsAttributes());
        	for(int i = 0; i < numberAttributes; i++) {
        		st.push(e.getChild(i));
        	}
        }*/
    }


    /**
     * Find the least general functional dependencies violated by t1 and t2
     * and add update the negative cover accordingly.<br/>
     * Note: t1 and t2 must have the same length.
     *
     * @param t1 An ObjectArrayList with the values of one entry of the relation.
     * @param t2 An ObjectArrayList with the values of another entry of the relation.
     */
    private static void violatedFds(List<String> t1, List<String> t2) {
        OpenBitSet equalAttr = new OpenBitSet();
        equalAttr.set(1, numberAttributes + 1);
        OpenBitSet diffAttr = new OpenBitSet();
        for (int i = 0; i < t1.size(); i++) {
            Object val1 = t1.get(i);
            Object val2 = t2.get(i);
            // Handling of null values. Currently assuming NULL values are equal.
            if (val1 == null && val2 == null) {
                continue;
            } else if ((val1 == null && val2 != null) || !(val1.equals(val2))) {
                // OpenBitSet start with 1 for first attribute
                diffAttr.set(i + 1);
            }
        }
        equalAttr.andNot(diffAttr);
        //System.out.println(" equal: " + equalAttr.toString());
        //System.out.println(" diff: " + equalAttr.toString());
        for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
            negCoverTree.addFunctionalDependency(equalAttr, a);
        }
        //negCoverTree.printDependencies();
    }


    /**
     * Fetch the data from the database and keep it as List of Lists.
     *
     * @throws AlgorithmExecutionException
     * @throws AlgorithmConfigurationException
     */
    private static void loadData() {
        tuples = new ArrayList<List<String>>();
        
        List<Row> table = df.collectAsList();
        numberTuples = table.size();

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

    private static void setColumnIdentifiers() {
        columnIdentifiers = new ArrayList<ColumnIdentifier>(
                numberAttributes);
        for (int i = 0; i < numberAttributes; i++) {
            columnIdentifiers.add(new ColumnIdentifier(datasetFile,
                    columnNames[i]));
        }
    }

    private static void addAllDependenciesToResultReceiver(FDTreeElement fds, OpenBitSet activePath) {
        /*if (this.fdResultReceiver == null) {
            return;
        }*/
        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (fds.isFd(attr - 1)) {
                int j = 0;
                ColumnIdentifier[] columns = new ColumnIdentifier[(int) activePath.cardinality()];
                for (int i = activePath.nextSetBit(0); i >= 0; i = activePath.nextSetBit(i + 1)) {
                    columns[j++] = columnIdentifiers.get(i - 1);
                }
                //ColumnCombination colCombination = new ColumnCombination(columns);
                //FunctionalDependency fdResult = new FunctionalDependency(colCombination, columnIdentifiers.get((int) attr - 1));
//				System.out.println(fdResult.toString());
                //fdResultReceiver.receiveResult(fdResult);
                /*for(int i = 0; i < columns.length; i++)
                	System.out.println(columns[i].toString());
                System.out.println("-----------\n");*/
                fdCount++;
            }
        }

        for (int attr = 1; attr <= numberAttributes; attr++) {
            if (fds.getChild(attr - 1) != null) {
                activePath.set(attr);
                addAllDependenciesToResultReceiver(fds.getChild(attr - 1), activePath);
                activePath.clear(attr);
            }
        }
    }


    /**
     * Add all functional Dependencies to the FunctionalDependencyResultReceiver.
     * Do nothing if the object does not have a result receiver.
     *
     * @throws CouldNotReceiveResultException
     */
    private static void addAllDependenciesToResultReceiver() {
        /*if (fdResultReceiver == null) {
            return;
        }*/
        addAllDependenciesToResultReceiver(posCoverTree, new OpenBitSet());
    }
}