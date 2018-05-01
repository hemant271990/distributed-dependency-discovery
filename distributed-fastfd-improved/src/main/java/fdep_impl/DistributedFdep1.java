package fdep_impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.lucene.util.OpenBitSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import fdep_helper.*;
import scala.Tuple2;

public class DistributedFdep1 implements Serializable{

	public static Dataset<Row> df = null;
	public static String[] columnNames = null;
	public static long numberTuples = 0;
	public static Integer numberAttributes = 0;
	public static JavaSparkContext sc;
	public static List<Integer> count_dependency;
	public static String datasetFile = null;
	public static int fdCount = 0;
	public static int numPartitions = 1;
	
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
    //private static ArrayList<List<String>> tuples;

    //private static FunctionalDependencyResultReceiver fdResultReceiver;

    public static void execute() {
    	long t1 = System.currentTimeMillis();
    	initialize();
        long t2 = System.currentTimeMillis();
        System.out.println(" initialize time(s): " + (t2-t1)/1000);
        
        negativeCover();
        long t3 = System.currentTimeMillis();
        System.out.println(" negativeCover time(s): " + (t3-t2)/1000);
        
        //tuples = null;
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
    	//int numPartitions = 7;
    	final Broadcast<Integer> b_numPartitions = sc.broadcast(numPartitions);
    	final Broadcast<Integer> b_numberAttributes = sc.broadcast(numberAttributes);
    	negCoverTree = new FDTree(numberAttributes);
    	
    	JavaRDD<Row> tableRDD = df.javaRDD().repartition(56);
    	JavaPairRDD<Integer, Row> rowMap = tableRDD.mapToPair(
    			new PairFunction<Row, Integer, Row>() {
	    		  public Tuple2<Integer, Row> call(Row r) { 
	    			  return new Tuple2<Integer, Row>((int)(Math.random()*b_numPartitions.value()), r); 
	    			  }
    		});
    	JavaPairRDD<Integer, Iterable<Row>> pairsPartition = rowMap.groupByKey();
    	JavaPairRDD<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>> joinedPairs = pairsPartition.cartesian(pairsPartition);
    	joinedPairs = joinedPairs.filter(new Function<Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>>, Boolean> () {
    		public Boolean call(Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>> t) {
    			if(t._1._1 > t._2._1)
    				return false;
    			return true;
    		}
    	});
    	/*JavaRDD<FDTree> differenceSetsRDD = joinedPairs.map(
    			new Function<Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>>, FDTree>() {
    				public FDTree call(Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>> t) {
    					//LinkedList<OpenBitSet> result_l = new LinkedList<OpenBitSet>();
    					FDTree negCoverTree = new FDTree(b_numberAttributes.value());
    					Iterable<Row> rl1 = t._1._2;
    					Iterable<Row> rl2 = t._2._2;
    					for(Row r1 : rl1) {
    						for(Row r2 : rl2) {
    							
    							OpenBitSet equalAttr = new OpenBitSet();
    					        equalAttr.set(1, b_numberAttributes.value() + 1);
    					        OpenBitSet diffAttr = new OpenBitSet();
    					        for (int i = 0; i < b_numberAttributes.value(); i++) {
    					            long val1 = r1.getLong(i);
    					            long val2 = r2.getLong(i);
    					            // Handling of null values. Currently assuming NULL values are equal.
    					            if (val1 != val2) {
    					                // OpenBitSet start with 1 for first attribute
    					                diffAttr.set(i + 1);
    					            }
    					        }
    					        equalAttr.andNot(diffAttr);
    					        for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
    					            negCoverTree.addFunctionalDependency(equalAttr, a);
    					        }
    					        //result_l.add(diffAttr);
    						}
    					}
    					//return result_l;
    					return negCoverTree;
    				}
    			});*/
    	//System.out.println("joinedPairs count: " + joinedPairs.count());
    	JavaRDD<HashSet<OpenBitSet>> differenceSetsRDD = joinedPairs.map(
    			new Function<Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>>, HashSet<OpenBitSet>>() {
    				public HashSet<OpenBitSet> call(Tuple2<Tuple2<Integer, Iterable<Row>>, Tuple2<Integer, Iterable<Row>>> t) {
    					HashSet<OpenBitSet> result_l = new HashSet<OpenBitSet>();
    					//FDTree negCoverTree = new FDTree(b_numberAttributes.value());
    					Iterable<Row> rl1 = t._1._2;
    					Iterable<Row> rl2 = t._2._2;
    					for(Row r1 : rl1) {
    						for(Row r2 : rl2) {
    							
    							OpenBitSet equalAttr = new OpenBitSet();
    					        equalAttr.set(1, b_numberAttributes.value() + 1);
    					        OpenBitSet diffAttr = new OpenBitSet();
    					        for (int i = 0; i < b_numberAttributes.value(); i++) {
    					            long val1 = r1.getLong(i);
    					            long val2 = r2.getLong(i);
    					            // Handling of null values. Currently assuming NULL values are equal.
    					            if (val1 != val2) {
    					                // OpenBitSet start with 1 for first attribute
    					                diffAttr.set(i + 1);
    					            }
    					        }
    					        /*equalAttr.andNot(diffAttr);
    					        for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
    					            negCoverTree.addFunctionalDependency(equalAttr, a);
    					        }*/
    					        result_l.add(diffAttr);
    						}
    					}
    					return result_l;
    					//return negCoverTree;
    				}
    			});
    	JavaRDD<OpenBitSet> combinedDiffSetRDD = differenceSetsRDD.flatMap(new FlatMapFunction<HashSet<OpenBitSet>, OpenBitSet>() {
        	  public Iterator<OpenBitSet> call(HashSet<OpenBitSet> l) {
	      		  List<OpenBitSet> result = new LinkedList<OpenBitSet>();
	      		  for(OpenBitSet i : l) {
	      			  result.add(i);
	      		  }
	      		  return result.iterator(); 
      		  }
      	});
    	
    	//List<FDTree> diff_attr_list = differenceSetsRDD.collect();
    	List<OpenBitSet> diff_attr_list = combinedDiffSetRDD.distinct().collect();
    	
    	/*ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
		  out = new ObjectOutputStream(bos);   
		  out.writeObject(diff_attr_list);
		  out.flush();
		  byte[] yourBytes = bos.toByteArray();
		  System.out.println("Java diff_attr_list serialization length in bytes " + yourBytes.length);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		  try {
		    bos.close();
		  } catch (IOException ex) {
		    // ignore close exception
		  }
		}*/
    	
    	System.out.println(" size: " + diff_attr_list.size());
    	/*for(FDTree fdt : diff_attr_list) {
    		negCoverTree.mergeTree(fdt);
    		//fdt.filterSpecializations();
    		//fdt.printDependencies();
    		//System.out.println(" ---\n");
    	}*/
    	for(OpenBitSet diff_attr : diff_attr_list) {
    		OpenBitSet equalAttr = new OpenBitSet();
	        equalAttr.set(1, numberAttributes + 1);
    		equalAttr.andNot(diff_attr);
	        for (int a = diff_attr.nextSetBit(0); a >= 0; a = diff_attr.nextSetBit(a + 1)) {
	            negCoverTree.addFunctionalDependency(equalAttr, a);
	        }
    	}
    	negCoverTree.filterSpecializations();
    	//negCoverTree.printDependencies();
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
        for (int a = diffAttr.nextSetBit(0); a >= 0; a = diffAttr.nextSetBit(a + 1)) {
            negCoverTree.addFunctionalDependency(equalAttr, a);
        }
    }


    /**
     * Fetch the data from the database and keep it as List of Lists.
     *
     * @throws AlgorithmExecutionException
     * @throws AlgorithmConfigurationException
     */
    private static void loadData() {
        //tuples = new ArrayList<List<String>>();
        
        //List<Row> table = df.collectAsList();
        df.cache();
        numberTuples = df.count();

        /*int tupleId = 0;
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
        }*/
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