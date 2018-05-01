package fastdc_impl;

/*import ETL.relational.Sparkdf;
import constraint.SparkDC;
import constraint.SparkPredicate;
import constraint.SparkPredicate.OpType;*/
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/*import ETL.relational.Literal;
import ETL.relational.Obj;
import ETL.relational.Obj.ObjectType;
import ETL.relational.Cell;*/
import scala.Serializable;
import scala.Tuple2;
import fastdc_helper.*;
import fastdc_helper.Obj.ObjectType;
import fastdc_helper.SparkPredicate.OpType;

import java.util.*;

public class SparkFastDC implements Serializable {
    public static Sparkdf sparkdf;
    public static List<SparkDC> DCs;
    public static JavaSparkContext jsc = null;
    public static String datasetFile = null;
    public static Dataset<Row> df = null;
    public static int numberAttributes;
    public static long numberTuples;
    public static String[] columnNames;
    
    // Process self-join using distributed partitioned
    public static int numReducers = 55;
    public static int sideLen = 10;
    protected static int maxMSC = 5;
    private static int[][] selfJoinReducerGroups;

    // RDD: {(S, {(S, P, O)})}
    public static JavaPairRDD<Integer, Iterable<Cell>> tableRDD;
    protected static List<String> allP;
    protected static Map<String, ObjectType> objectTypeMap;

    protected static List<SparkPredicate> predicateSpace;
    protected static List<BitSet> evidenceSet;
    protected static Map<Integer, BitSet> predicateCovers;
    protected static Set<MSC> minimalSetCover;

    protected static List<SparkPredicate> predicateSpaceN;
    protected static List<BitSet> evidenceSetN;
    protected static Map<Integer, BitSet> predicateCoversN;
    protected static Set<MSC> minimalSetCoverN;

    protected static int numP;
    protected static Map<String, Integer> indexP;

    private static final Logger logger = LogManager.getLogger(SparkFastDC.class);

    /*public SparkFastDC(Sparkdf sparkdf, JavaPairRDD<Integer, Iterable<Cell>> tableRDD, int numReducers) {
        this.sparkdf = sparkdf;
        this.tableRDD = tableRDD;
        this.numReducers = numReducers;
        this.allP = sparkdf.allP;
        this.numP = sparkdf.numP;
        this.indexP = sparkdf.indexP;
        this.objectTypeMap = sparkdf.allPTypes;

        selfJoinReducerGroups = computeParallelSettingSelfJoin(numReducers);

    }

    public SparkFastDC(Map<String, ObjectType> objectTypeMap, int numReducers) {
        this.numReducers = numReducers;
        this.objectTypeMap = objectTypeMap;
        selfJoinReducerGroups = computeParallelSettingSelfJoin(numReducers);
    }

    public SparkFastDC(JavaPairRDD<Integer, Iterable<Cell>> tableRDD, Map<String, ObjectType> objectTypeMap, int numReducers) {
        this.tableRDD = tableRDD;
        this.numReducers = numReducers;
        this.objectTypeMap = objectTypeMap;
        selfJoinReducerGroups = computeParallelSettingSelfJoin(numReducers);
    }*/

    public static void execute() {
        System.out.println("FastDC on Spark: Discovering...");
        
        sparkdf = new Sparkdf(df);
        sparkdf.analyze();
        //sparkdf.toTable();
        allP = sparkdf.allP;
        numP = sparkdf.numP;
        indexP = sparkdf.indexP;
        objectTypeMap = sparkdf.allPTypes;
        //JavaRDD rdd = jsc.parallelize(sparkdf.tuples);
        //JavaPairRDD<Integer, Iterable<Cell>> pairRdd = JavaPairRDD.fromJavaRDD(rdd);
        //final String index = pairRdd.first()._1.toString();
        //pairRdd.first()._2.iterator().forEachRemaining(x -> System.out.println(index +":" +x.column +"#"+" "+ x.toString(false)));
        tableRDD = jsc.parallelizePairs(sparkdf.toRDD());
        /*tableRDD = df.javaRDD().zipWithIndex().mapToPair(new PairFunction<Tuple2<Row, Long>, Integer, Iterable<Cell>>(){
        	public Tuple2<Integer, Iterable<Cell>> call(Tuple2<Row, Long> t){
        		List<Cell> cl = new ArrayList<Cell>();
        		int rowIndex = t._2.intValue(); // new Long(r.getLong(r.length()-1)).intValue();
        		Row r = t._1;
        		for(int i = 0; i < r.length(); i++)
        			cl.add(new Cell(rowIndex, i, "integer", new Long(r.getLong(i)).intValue()));
        		return new Tuple2<Integer, Iterable<Cell>>(rowIndex, cl);
        	}
        });*/
        //SparkFastDC dc = new SparkFastDC(sparkdf, pairRdd, numReducers);
        selfJoinReducerGroups = computeParallelSettingSelfJoin(numReducers);
        
        analyzeTable();

        buildPredicateSpace();

        buildEvidenceSet();

        predicateCovers = computeCovers(predicateSpace, evidenceSet);
        //predicateCoversN = computeCovers(predicateSpaceN, evidenceSetN);

        // System.out.println("Predicate Cover (1 tuple): ");
        // System.out.println(predicateCoversN);

        findMSC();

        // System.out.println("MSC (1 tuple): ");
        // System.out.println(minimalSetCoverN);

        DCs = MSCtoDC(minimalSetCover,  predicateSpace, 2);

        System.out.println("-----------------------------------------------------");
        System.out.println("FastDC on Spark: Discovered DCs count: " + DCs.size());
        //DCs.forEach(dc -> System.out.println(dc));
        System.out.println("-----------------------------------------------------");
    }
    
    public static int[][] computeParallelSettingSelfJoin(int numberOfCores) {
        int B = 0;
        while (B * (B + 1) / 2 <= numberOfCores) ++ B;
        -- B;

        int[][] group = new int[B][B];
        /*
        System.out.println(group.length);
        System.out.println(group[0].length);
        */

        int first = 0;
        for (int i = 1; i <= B; ++ i) {
            int k = 0;
            for (int j = 0; j < i; ++ j)
                group[i - 1][k ++] = first + j;
            int corner = first + i - 1;
            for (int j = i; j < B; ++ j) {
                corner += j;
                group[i - 1][k ++] = corner;
            }
            first += i;
        }

        return group;
    }

    protected static BitSet computeOnePairEvidence(Obj[] tupleA, Obj[] tupleB,
                                                   List<SparkPredicate> predicateSpace) {
        BitSet evidence = new BitSet(predicateSpace.size());

        for (int index = 0; index < predicateSpace.size(); ++ index) {
            SparkPredicate predicate = predicateSpace.get(index);
            ObjectType predicateType = predicate.type;
			//System.out.println(predicate);
			//System.out.println("Predicate type: "+predicateType);
            /*if (predicateType != tupleA[predicate.leftIndex].type || predicateType != tupleB[predicate.rightIndex].type) {
                if (tupleA[predicate.leftIndex].type == tupleB[predicate.rightIndex].type) {
                    predicateType = tupleA[predicate.leftIndex].type;
                } else {
//                    System.out.println("Incompatible types. Predicate index in the space: " + index
//                            + ", Property type in Schema: " + predicateType +
//                            ", TupleA:" + tupleA[predicate.leftIndex].type +
//                            ", TupleB: " + tupleB[predicate.rightIndex].type + "\n" +
//                            "tupleA[idx] =" + tupleA[predicate.leftIndex].toString() + "\n" +
//                            "tupleB[idx] =" + tupleB[predicate.rightIndex].toString());
                    return null;
                }
            }*/
			//System.out.println("Predicate type later: "+predicateType);

            //if (predicateType == ObjectType.IntegerLiteral || predicateType == ObjectType.DoubleLiteral || predicateType == ObjectType.DateLiteral) {
            /*if (predicateType.toString().equals("IntegerLiteral") || predicateType.toString().equals("DoubleLiteral") || predicateType.toString().equals("DateLiteral")) {
                int cmp = ((Literal)tupleA[predicate.leftIndex]).compareTo((Literal)tupleB[predicate.rightIndex]);
                if (cmp == 0 && (predicate.op.toString().equals("EQ")))
                    evidence.set(index);
                if (cmp == 1 && (predicate.op.toString().equals("GT")))
                    evidence.set(index);
                if (cmp == -1 && (predicate.op.toString().equals("LT")))
                    evidence.set(index);
            } else {
                if (predicate.op.toString().equals("EQ") && tupleA[predicate.leftIndex].equals(tupleB[predicate.rightIndex]))
                    evidence.set(index);
                if (predicate.op.toString().equals("NEQ") && !tupleA[predicate.leftIndex].equals(tupleB[predicate.rightIndex]))
                    evidence.set(index);
            }*/
			//if(predicateType.toString().equals("IntegerLiteral")){
            if(predicateType == ObjectType.IntegerLiteral){
            	Integer a = Integer.parseInt(((Literal)tupleA[predicate.leftIndex]).value.toString());
            	Integer b = Integer.parseInt(((Literal)tupleB[predicate.rightIndex]).value.toString());
            	//System.out.println("Integers: "+a+" "+b);
            	int cmp = a.compareTo(b);
            	if (cmp == 0 && (predicate.op.toString().equals("EQ")))
                    evidence.set(index);
                if (cmp == 1 && (predicate.op.toString().equals("GT")))
                    evidence.set(index);
                if (cmp == -1 && (predicate.op.toString().equals("LT")))
                    evidence.set(index);
            //} else if(predicateType.toString().equals("DoubleLiteral")) {
            } else if(predicateType == ObjectType.DoubleLiteral) {
            	Double a = Double.parseDouble(((Literal)tupleA[predicate.leftIndex]).value.toString());
            	Double b = Double.parseDouble(((Literal)tupleB[predicate.rightIndex]).value.toString());
            	//System.out.println("Doubles: "+a+" "+b);
            	int cmp = a.compareTo(b);
            	if (cmp == 0 && (predicate.op.toString().equals("EQ")))
                    evidence.set(index);
                if (cmp == 1 && (predicate.op.toString().equals("GT")))
                    evidence.set(index);
                if (cmp == -1 && (predicate.op.toString().equals("LT")))
                    evidence.set(index);
            //} else if(predicateType.toString().equals("StringLiteral")) {
            } else if(predicateType == ObjectType.StringLiteral) {
            	String a = ((Literal)tupleA[predicate.leftIndex]).value.toString();
            	String b = ((Literal)tupleB[predicate.rightIndex]).value.toString();
            	//System.out.println("Strings: "+a+" "+b);
            	if (predicate.op.toString().equals("EQ") && a.equals(b))
                    evidence.set(index);
                if (predicate.op.toString().equals("NEQ") && !a.equals(b))
                    evidence.set(index);
            }            
        }
		//DEBUG
        /*for(int i = 0; i < tupleA.length; i++)
			System.out.print(((Literal)tupleA[i]).value+" ");
        System.out.println();
        for(int i = 0; i < tupleB.length; i++)
        	System.out.print(((Literal)tupleB[i]).value+" ");
        System.out.println("-----");*/
        return evidence;
    }
    
    // DEBUG
    protected static List<BitSet> computeEvidences(JavaPairRDD<Integer, Iterable<Cell>> tuplesRDD, int numP, List<SparkPredicate> predicateSpace) {
    	List<Tuple2<Integer, Iterable<Cell>>> table = tuplesRDD.collect();
    	Set<BitSet> partialEvidenceSet = new HashSet<>();
    	for(int i = 0; i < table.size(); i++) {
    		for(int j = i+1; j < table.size(); j++) {
    			Obj[] tupleA = new Obj[numP];
            	table.get(i)._2.forEach(
                    cell -> {
                        int index = cell.column;
                        tupleA[index] = cell.getLiteralValue();
                    }
            	);
            	Obj[] tupleB = new Obj[numP];
            	table.get(j)._2.forEach(
                    cell -> {
                        int index = cell.column;
                        tupleB[index] = cell.getLiteralValue();
                    }
            	);
            	
            	BitSet evidence = computeOnePairEvidence(tupleA, tupleB, predicateSpace);
                if (evidence != null) partialEvidenceSet.add(evidence);
    		}
    	}
    	List<BitSet> completeEvidence = new ArrayList<BitSet>();
    	for(BitSet b : partialEvidenceSet)
    		completeEvidence.add(b);
    	return completeEvidence;
    }

    protected static List<BitSet> computeEvidencesBySelfJoin(JavaPairRDD<Integer, Iterable<Cell>> tuplesRDD,
                                                             int numP,
                                                             List<SparkPredicate> predicateSpace,
                                                             Map<String, Integer> indexP,
                                                             int numReducers,
                                                             int[][] selfJoinReducerGroups) {

        int numSelfJoinGroups = selfJoinReducerGroups.length;
        System.out.println("# Self Join reducer groups = "+ numSelfJoinGroups);
        // RDD: {(S, {(S, P, O)})} -> {{(S, P, O)}} -> {(RID, (L/R/C, {(S, P, O)}))}
        /*JavaPairRDD<Integer, Tuple2<Integer, Iterable<Cell>>> assignedTableRDD = tuplesRDD.map(
                pair -> pair._2()
        ).flatMapToPair(
                cells -> {
                    List<Tuple2<Integer, Tuple2<Integer, Iterable<Cell>>>> ret = new ArrayList<>();
                    // Generate a random number
                    int assignedToGroup = new Random().nextInt(numSelfJoinGroups);
                    int[] reducersGroup = selfJoinReducerGroups[assignedToGroup];
                    int cornerMachineIndex = assignedToGroup; // Corner = group inde
                    for (int i = 0; i < reducersGroup.length; ++ i) {
                        int reducerID = reducersGroup[i];

                        // Choose (Left, Corner, Right) according to the index of the machine
                        int lrc = (i < cornerMachineIndex ? -1 : (i > cornerMachineIndex ? 1 : 0));

                        ret.add(new Tuple2<>(reducerID, new Tuple2<>(lrc, cells)));
                    }
                    return ret.iterator();
                }
        );*/
        
    	final Broadcast<Integer> b_sideLen = jsc.broadcast(sideLen);
    	final Broadcast<Integer> b_numReducers = jsc.broadcast(numReducers);
    	final Broadcast<List<SparkPredicate>> b_predicateSpace = jsc.broadcast(predicateSpace);
        JavaPairRDD<Integer, Tuple2<Integer, Iterable<Cell>>> assignedTableRDD = tableRDD.map(
                pair -> pair._2()
        ).flatMapToPair(new PairFlatMapFunction< Iterable<Cell>, Integer, Tuple2<Integer, Iterable<Cell>>>() {
    		public Iterator<Tuple2<Integer, Tuple2<Integer, Iterable<Cell>>>> call( Iterable<Cell> r) {
    			
    			List<Tuple2<Integer, Tuple2<Integer, Iterable<Cell>>>> result = new ArrayList<Tuple2<Integer, Tuple2<Integer, Iterable<Cell>>>>();
    			int a = (int)(Math.random()*b_sideLen.value()) + 1;
    			for(int p = 1; p < a; p++) {
    				int rid = getReducerId(p, a, b_sideLen.value());
    				Tuple2<Integer, Tuple2<Integer, Iterable<Cell>>> t = 
    						new Tuple2<Integer, Tuple2<Integer, Iterable<Cell>>>(rid, new Tuple2<Integer, Iterable<Cell>>(-1, r));
    				result.add(t);
    			}
    			int rid2 = getReducerId(a, a, b_sideLen.value());
    			Tuple2<Integer, Tuple2<Integer, Iterable<Cell>>> t2 = 
						new Tuple2<Integer, Tuple2<Integer, Iterable<Cell>>>(rid2, new Tuple2<Integer, Iterable<Cell>>(0, r));
				result.add(t2);
				for(int q = a+1; q <= b_sideLen.value(); q++) {
					int rid = getReducerId(a, q, b_sideLen.value());
					Tuple2<Integer, Tuple2<Integer, Iterable<Cell>>> t = 
    						new Tuple2<Integer, Tuple2<Integer, Iterable<Cell>>>(rid, new Tuple2<Integer, Iterable<Cell>>(1, r));
    				result.add(t);
				}
    			return result.iterator();	
    		}
    	});

        // Partitioner based on the reducer ID
        JavaPairRDD<Integer, Tuple2<Integer, Iterable<Cell>>> partitionedTableRDD = assignedTableRDD.partitionBy(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                return (int)key-1;
            }

            @Override
            public int numPartitions() {
                return b_numReducers.value();
            }
        });

        //partitionedTableRDD.cache();

        // RDD: {(RID, (L/R/C, {(S, P, O)}))} -> {evidence}
        JavaRDD<BitSet> evidencesRDD = partitionedTableRDD.mapPartitions(
                iterator -> {
                    // TODO: need to think about here carefully
                    List<Obj[]> tuplesL = new ArrayList<>();
                    List<Obj[]> tuplesR = new ArrayList<>();
                    List<Obj[]> tuplesC = new ArrayList<>();

                    while (iterator.hasNext()) {
                        Tuple2<Integer, Iterable<Cell>> triplesWithLRC = iterator.next()._2();
                        int lrc = triplesWithLRC._1();

                        Obj[] tuple = new Obj[numP];
                        triplesWithLRC._2().forEach(
                                cell -> {
                                    int index = cell.column;
                                    tuple[index] = cell.getLiteralValue();
                                }
                        );

                        if (lrc == -1)
                            tuplesL.add(tuple);
                        else if (lrc == 1)
                            tuplesR.add(tuple);
                        else
                            tuplesC.add(tuple);
                    }

                    Set<BitSet> partialEvidenceSet = new HashSet<>();

                    for (Obj[] tupleA : tuplesL) {
                        for (Obj[] tupleB : tuplesR) {
                            BitSet evidence = computeOnePairEvidence(tupleA, tupleB, b_predicateSpace.value());
                            if (evidence != null) partialEvidenceSet.add(evidence);
                        }
                    }

                    for (int i = 0; i < tuplesC.size(); ++ i) {
                        Obj[] tupleA = tuplesC.get(i);
                        for (int j = i + 1; j < tuplesC.size(); ++ j) {
                            Obj[] tupleB = tuplesC.get(j);
                            BitSet evidence = computeOnePairEvidence(tupleA, tupleB, b_predicateSpace.value());
                            if (evidence != null) partialEvidenceSet.add(evidence);
                        }
                    }

                    //System.out.println("size(partialEvidenceSet) = " + partialEvidenceSet.size());
                    return partialEvidenceSet.iterator();
                }
        );

        // RDD: {evidence} -> {evidence}
        JavaRDD<BitSet> evidenceSetRDD = evidencesRDD.distinct();
        //JavaRDD<BitSet> evidenceSetRDD = evidencesRDD.mapToPair(e -> new Tuple2<BitSet, Integer>(e, 1)).reduceByKey((a, b) -> a + b).keys();
        List<BitSet> evidences = evidenceSetRDD.collect();

        //partitionedTableRDD.unpersist();

        return evidences;
    }

    private static int getReducerId(int i, int j, int b_sideLen) {
    	int t1 = (2*b_sideLen - i + 2)*(i-1)/2;
    	int t2 = j - i + 1;
    	return t1 + t2;
    }
    
    protected static List<BitSet> computeEvidences(JavaPairRDD<Integer, Iterable<Cell>> tuplesRDD,
                                                   int numP,
                                                   List<SparkPredicate> predicateSpace,
                                                   Map<String, Integer> indexP) {
        JavaRDD<BitSet> evidence = tuplesRDD
                .map(
                        pair -> {
                            Iterable<Cell> cells = pair._2();

                            Obj[] tuple = new Obj[numP];
                            cells.forEach(
                                    cell -> {
                                        int index = cell.column;
                                        tuple[index] = cell.getLiteralValue();
                                    }
                            );

                            return computeOnePairEvidence(tuple, tuple, predicateSpace);
                        }
                ).filter(Objects::nonNull); // Make sure we do not have nulls.

        return evidence.mapToPair(e -> new Tuple2<BitSet, Integer>(e, 1)).reduceByKey((a, b) -> a + b).keys().collect();
    }

    private static void buildEvidenceSet() {
        System.out.println("FastDC on Spark: Building Evidence Set...");

        evidenceSet = computeEvidencesBySelfJoin(tableRDD, numP, predicateSpace, indexP, numReducers, selfJoinReducerGroups);
        //evidenceSet = computeEvidences(tableRDD, numP, predicateSpace);
        //evidenceSetN = computeEvidences(tableRDD, numP, predicateSpaceN, indexP);
        System.out.println("Size of evidenceSet = "+ evidenceSet.size());
        //System.out.println("Size of evidenceSetN = "+ evidenceSetN.size());

//        // Print out all evidence
        /*System.out.println("Evidence (2 tuples): ("+ evidenceSet.size()+")");
        for (BitSet evidence : evidenceSet) {
            String bitString = "";
            for (int i = 0; i < predicateSpace.size(); ++ i)
                bitString += (evidence.get(i) ? "1" : "0");
            for (int i = 0; i < predicateSpace.size(); ++ i)
                bitString += (evidence.get(i) ? predicateSpace.get(i) : "");
            System.out.println(bitString);
            System.out.println(evidence);
        }*/
//        System.out.println("Evidence (1 tuple): ("+ evidenceSetN.size()+")");
//        for (BitSet evidence : evidenceSetN) {
//            String bitString = "";
//            for (int i = 0; i < predicateSpaceN.size(); ++ i)
//                bitString += (evidence.get(i) ? "1" : "0");
//            System.out.println(bitString);
//            System.out.println(evidence);
//        }

    }

    private static void findMSC() {
        minimalSetCover = new HashSet<>();
        //minimalSetCoverN = new HashSet<>();

        System.out.println("FastDC on Spark: Finding Minimal Set Covers...");
        searchMSC(minimalSetCover, predicateSpace, evidenceSet, predicateCovers,
                0, new MSC(predicateSpace.size()), new BitSet(evidenceSet.size()));
        //System.out.println("FastDC on Spark: Finding Minimal Set Covers for NEQ...");
        //searchMSC(minimalSetCoverN, predicateSpaceN, evidenceSetN, predicateCoversN,
          //      0, new MSC(predicateSpaceN.size()), new BitSet(evidenceSetN.size()));
    }

    protected static void maintainMinimality(Set<MSC> minimalSetCover, MSC candidate) {
        for (Iterator<MSC> it = minimalSetCover.iterator(); it.hasNext(); ) {
            MSC msc = it.next();
            if (msc.mustMinimal) continue;

            BitSet check = (BitSet)candidate.cover.clone();
            check.and(msc.cover);

            if (candidate.cover.cardinality() == check.cardinality())
                it.remove();
        }
        minimalSetCover.add(new MSC(candidate));
    }

    protected static boolean checkMinimality(Set<MSC> minimalSetCover, MSC candidate) {
        for (MSC msc : minimalSetCover) {
            if (msc.cover.cardinality() >= candidate.cover.cardinality()) continue;
            BitSet check = (BitSet)candidate.cover.clone();
            check.and(msc.cover);
            if (msc.cover.cardinality() == check.cardinality()) return false;
        }
        return true;
    }

    protected static void searchMSC(Set<MSC> minimalSetCover, List<SparkPredicate> predicateSpace,
                             List<BitSet> evidenceSet, Map<Integer, BitSet> predicateCovers,
                             int start, MSC candidate, BitSet covered) {
    	if (candidate.cover.cardinality() > maxMSC) return;
        if (!checkMinimality(minimalSetCover, candidate)) return;

        if (covered.cardinality() == evidenceSet.size()) {
            maintainMinimality(minimalSetCover, candidate);
            //System.out.println(candidate.toString());
            return;
        }

        int k = start;
        while (k < predicateSpace.size()) {
        	// System.out.println("SearchMSC: "+k);
            int delta = predicateSpace.get(k).type.getPredicateSpaceSize();
            boolean allEmpty = true;
            for (int offset = 0; offset < delta; ++ offset) {
                if (candidate.cover.get(k + offset)) {
                    allEmpty = false;
                    break;
                }
            }
            if (allEmpty) {
                for (int offset = 0; offset < delta; ++ offset) {
                    BitSet nextCovered = (BitSet)covered.clone();
                    nextCovered.or(predicateCovers.get(k + offset));
                    // have to cover at least one evidence which hasn't covered before
                    if (nextCovered.cardinality() > covered.cardinality()) {
                        candidate.cover.set(k + offset);
                        searchMSC(minimalSetCover, predicateSpace, evidenceSet, predicateCovers,
                                k + delta, candidate, nextCovered);
                        candidate.cover.clear(k + offset);
                    }
                }
            }
            k += delta;
        }
    }
    
    protected static void analyzeTable() {
        System.out.println("FastDC on Spark: Analyzing the Table...");
        System.out.println("Approximate tableRDD count: "+
                tableRDD.countApprox(10000, 0.9).toString());

        // extract all properties (schema of the table)
        // TODO: hard code here
        //Set<String> candidateProperties = new HashSet<>();
        //tableRDD.first()._2().forEach(cell -> candidateProperties.add(allP.get(cell.column)));
        //tableRDD.first()._2().forEach(logger::debug);
        //allP = new ArrayList<>(candidateProperties);
        //numP = allP.size();

        /*System.out.println("-------------------------------------");
        System.out.println("All Properties:");
        for (String P : allP) {
            System.out.println(P.toString()+" --> "+ objectTypeMap.get(P));
        }*/

        indexP = new HashMap<>();
        int index = 0;
        for (String p : allP) {
            indexP.put(p, index);
            ++ index;
        }
    }

    protected static void buildPredicateSpace() {

        System.out.println("FastDC on Spark: Building Predicate Space...");

        predicateSpace = new ArrayList<>();
        predicateSpaceN = new ArrayList<>();

        for (int x = 0; x < numP; ++ x) {
            String propertyX = allP.get(x);
            ObjectType objTypeX = objectTypeMap.get(propertyX);
            //System.out.println(objTypeX);
            for (int y = x; y < numP; ++ y) {
            	if(y != x)
            		continue;
                String propertyY = allP.get(y);
                ObjectType objTypeY = objectTypeMap.get(propertyY);
                if (objTypeX == objTypeY) {
					//System.out.println("PS: "+propertyX+" "+x);
                	//System.out.println("PS: "+propertyY+" "+y);
                    predicateSpace.add(new SparkPredicate(OpType.EQ, propertyX, propertyY, objTypeX, x, y));
                    predicateSpace.add(new SparkPredicate(OpType.NEQ, propertyX, propertyY, objTypeX, x, y));

                    if (x != y) {
                        predicateSpaceN.add(new SparkPredicate(OpType.EQ, propertyX, propertyY, objTypeX, x, y));
                        predicateSpaceN.add(new SparkPredicate(OpType.NEQ, propertyX, propertyY, objTypeX, x, y));
                    }

                    if (objTypeX == ObjectType.IntegerLiteral || objTypeX == ObjectType.DoubleLiteral || objTypeX == ObjectType.DateLiteral) {
                        predicateSpace.add(new SparkPredicate(OpType.LT, propertyX, propertyY, objTypeX, x, y));
                        predicateSpace.add(new SparkPredicate(OpType.LTE, propertyX, propertyY, objTypeX, x, y));
                        predicateSpace.add(new SparkPredicate(OpType.GT, propertyX, propertyY, objTypeX, x, y));
                        predicateSpace.add(new SparkPredicate(OpType.GTE, propertyX, propertyY, objTypeX, x, y));

                        if (x != y) {
                            predicateSpaceN.add(new SparkPredicate(OpType.LT, propertyX, propertyY, objTypeX, x, y));
                            predicateSpaceN.add(new SparkPredicate(OpType.LTE, propertyX, propertyY, objTypeX, x, y));
                            predicateSpaceN.add(new SparkPredicate(OpType.GT, propertyX, propertyY, objTypeX, x, y));
                            predicateSpaceN.add(new SparkPredicate(OpType.GTE, propertyX, propertyY, objTypeX, x, y));
                        }
                    }
                }
            }
        }

        System.out.println("-----------------------------------------");
        System.out.println("Predicate Space (2 tuples): ("+ predicateSpace.size()+")");
        //for(int i = 0; i < predicateSpace.size(); i++)
        //	System.out.println(i+" "+predicateSpace.get(i));
        //predicateSpace.forEach(System.out::println);
        System.out.println("Predicate Space (1 tuple): ("+ predicateSpaceN.size()+")");
        //predicateSpaceN.forEach(System.out::println);
    }

    protected static Map<Integer, BitSet> computeCovers(List<SparkPredicate> predicateSpace, List<BitSet> evidenceSet) {
        System.out.println("FastDC on Spark: Preprocessing the Evidence Set for Finding MSCs...");

        Map<Integer, BitSet> predicateCovers = new HashMap<>();

        for (int k = 0; k < predicateSpace.size(); ++ k) {
            BitSet cover = new BitSet(evidenceSet.size());
            predicateCovers.put(k, cover);
        }

        for (int i = 0; i < evidenceSet.size(); ++ i) {
            BitSet evi = evidenceSet.get(i);
            for (int k = evi.nextSetBit(0); k >= 0; k = evi.nextSetBit(k + 1)) {
                BitSet cover = predicateCovers.get(k);
                cover.set(i);
            }
        }

        return predicateCovers;
    }

    public static void generateDCs() {
        System.out.println("Generating DCs...");
        DCs = new ArrayList<>();
        DCs.addAll(MSCtoDC(minimalSetCover, predicateSpace, 2));
        DCs.addAll(MSCtoDC(minimalSetCoverN, predicateSpaceN, 1));
    }

    static SparkPredicate getComplimentingPredicate(List<SparkPredicate> currentPredicates, SparkPredicate targetPredicate) {
        for (SparkPredicate predicate : currentPredicates) {
            if (predicate.leftIndex != targetPredicate.leftIndex || predicate.rightIndex != targetPredicate.rightIndex)
                continue;
            if (targetPredicate.op == OpType.EQ && (predicate.op == OpType.LT || predicate.op == OpType.GT))
                return predicate;
            if ((targetPredicate.op == OpType.LT || targetPredicate.op == OpType.GT) && predicate.op == OpType.EQ)
                return predicate;
        }
        return null;
    }

    static SparkPredicate mergeComplimentingPredicates(SparkPredicate p1, SparkPredicate p2) {
        assert p1.op != p2.op;
        if (p1.op != OpType.EQ) {
            // Make sure the first predicate has the equality for simplicity.
            return mergeComplimentingPredicates(p2, p1);
        }
        if (p2.op == OpType.LT)
            p2.op = OpType.LTE;
        else
            p2.op = OpType.GTE;
        return p2;
    }

    protected static List<SparkDC> MSCtoDC(Set<MSC> minimalSetCover, List<SparkPredicate> predicateSpace, int numTuples) {
        System.out.println("FastDC on Spark: Converting MSCs to DCs...");

        List<SparkDC> DCs = new ArrayList<>();
        for (MSC msc : minimalSetCover) {
            List<SparkPredicate> predicates = new ArrayList<>();
            for (int x = msc.cover.nextSetBit(0); x >= 0; x = msc.cover.nextSetBit(x + 1)) {
                SparkPredicate truePredicate = predicateSpace.get(x);
                SparkPredicate complimentingPredicate = getComplimentingPredicate(predicates, truePredicate);
                SparkPredicate predicateToAdd = null;
                if (complimentingPredicate == null) {
                    predicateToAdd = truePredicate;
                } else {
                    predicateToAdd = mergeComplimentingPredicates(truePredicate, complimentingPredicate);
                    predicates.remove(complimentingPredicate);
                }
                predicates.add(predicateToAdd);
            }

            List<SparkPredicate> negatedPredicates = new ArrayList<>(predicates.size());
            for (SparkPredicate predicate : predicates) {
                negatedPredicates.add(predicate.negation());
            }

            SparkDC dc = new SparkDC(negatedPredicates, numTuples);
            DCs.add(dc);
        }
        return DCs;
    }

}
