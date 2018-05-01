package fastfd_impl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.apache.lucene.util.OpenBitSet;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.Int2IntArrayMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;


import fastfd_helper.*;

public class FastFD
{
	public static Dataset<Row> df = null;
	public static String[] columnNames = null;
	public static Integer numberTuples = 0;
	public static Integer numberAttributes = 0;
	public static JavaSparkContext sc;
	public static String datasetFile = null;
	public static boolean debugSysout = false; // for debugging
	public static int fdCount = 0;
			
	public static List<StrippedPartition> strippedPartitionGenerator() {
		
		List<Row> rows = df.collectAsList();
        numberTuples = rows.size();

        String nullValue = "null#" + Math.random();
        List<StrippedPartition> returnValue;
        Int2ObjectMap<Map<String, LongList>> translationMaps = new Int2ObjectOpenHashMap<Map<String, LongList>>();
        int tupleId = 0;
        
        //Read and paste data into TranslationMaps
        while (tupleId < numberTuples) {
           Row row = rows.get(tupleId);
           
           for (int i = 0; i < numberAttributes; i++) {
              // my_print(((Long)row.get(i)).intValue());

              String content = row.get(i).toString();
              if (null == content) {
                  content = nullValue;
              }
              
              Map<String, LongList> translationMap;
              if ((translationMap = translationMaps.get(i)) == null) {
                  translationMap = new HashMap<String, LongList>();
                  translationMaps.put(i, translationMap);
              }
              
              LongList element;
              if ((element = translationMap.get(content)) == null) {
                  element = new LongArrayList();
                  translationMap.put(content, element);
              }
              element.add(tupleId);
           }
              
           tupleId++;
           if(tupleId%100000 == 0) System.out.println("Copied tuple: " + tupleId);
        }
        

        //Read the lists and create the Stripped Partitions
        returnValue = new LinkedList<StrippedPartition>();
        for (int i : translationMaps.keySet()) {
        	StrippedPartition sp = new StrippedPartition(i);
            returnValue.add(sp);

            Map<String, LongList> toItterate = translationMaps.get(i);

            for (LongList it : toItterate.values()) {
                if (it.size() > 1) {
                    sp.addElement(it);
                }
            }

            // Clean up after work
            translationMaps.get(i).clear();
        }
        
        // to clean up
        translationMaps.clear();

        /*for(StrippedPartition sp : returnValue){
        	List<LongList> v = sp.getValues();
        	System.out.println(v.toString());
        }*/
        return returnValue;
	}
	
	public static List<AgreeSet> agreeSetGenerator(List<StrippedPartition> partitions) {

		boolean chooseAlternative1 = false;
	    boolean chooseAlternative2 = true;
	    
        if (debugSysout) {
            long sum = 0;
            for (StrippedPartition p : partitions) {
                System.out.println("-----");
                System.out.println("Attribut: " + p.getAttributeID());
                System.out.println("Anzahl Partitionen: " + p.getValues().size());
                sum += p.getValues().size();
            }
            System.out.println("-----");
            System.out.println("Summe: " + sum);
            System.out.println("-----");
        }

        Set<LongList> maxSets;
        /*if (chooseAlternative1) {
            maxSets = computeMaximumSetsAlternative(partitions);
        } else if (chooseAlternative2) {
            maxSets = computeMaximumSetsAlternative2(partitions);
        } else {
            maxSets = computeMaximumSets(partitions);
        }*/

        int spCount = 0;
        for(StrippedPartition sp : partitions){
        	List<LongList> v = sp.getValues();
        	spCount += v.size();
   		}
        maxSets = computeMaximumSets(partitions);
        /*System.out.println("====> MaxSets");
        for(LongList ll : maxSets){
        	System.out.println(ll.toString());
        }*/
        Set<AgreeSet> agreeSets = computeAgreeSets(calculateRelationships(partitions), maxSets, partitions);

        List<AgreeSet> result = new LinkedList<AgreeSet>(agreeSets);


        return result;
    }
	
	/*public static Set<LongList> computeMaximumSetsAlternative(List<StrippedPartition> partitions) {

        if (this.debugSysout) {
            System.out.println("\tstartet calculation of maximal partitions");
        }
        long start = System.currentTimeMillis();

        Set<LongList> sortedPartitions = new TreeSet<LongList>(new ListComparator());
        for (StrippedPartition p : partitions) {
            sortedPartitions.addAll(p.getValues());
        }
        Iterator<LongList> it = sortedPartitions.iterator();
        Long2ObjectMap<Set<LongList>> maxSets = new Long2ObjectOpenHashMap<Set<LongList>>();
        long remainingPartitions = sortedPartitions.size();
        if (this.debugSysout) {
            System.out.println("\tNumber of Partitions: " + remainingPartitions);
        }
        if (it.hasNext()) {
            LongList actuelList = it.next();
            long minSize = actuelList.size();
            Set<LongList> set = new HashSet<LongList>();
            set.add(actuelList);
            while ((actuelList = it.next()) != null && (actuelList.size() == minSize)) {
                if (this.debugSysout) {
                    System.out.println("\tremaining: " + --remainingPartitions);
                }
                set.add(actuelList);
            }
            maxSets.put(minSize, set);
            if (actuelList != null) {
                maxSets.put(actuelList.size(), new HashSet<LongList>());
                if (this.debugSysout) {
                    System.out.println("\tremaining: " + --remainingPartitions);
                }
                this.handleList(actuelList, maxSets, true);
                while (it.hasNext()) {
                    actuelList = it.next();
                    if (this.debugSysout) {
                        System.out.println("\tremaining: " + --remainingPartitions);
                    }
                    if (!maxSets.containsKey(actuelList.size()))
                        maxSets.put(actuelList.size(), new HashSet<LongList>());
                    this.handleList(actuelList, maxSets, true);
                }
            }
        }

        long end = System.currentTimeMillis();
        if (this.debugSysout)
            System.out.println("\tTime needed: " + (end - start));

        Set<LongList> max = this.mergeResult(maxSets);
        maxSets.clear();
        sortedPartitions.clear();

        return max;
    }
	
	public static Set<LongList> computeMaximumSetsAlternative2(List<StrippedPartition> partitions) {

        if (this.debugSysout) {
            System.out.println("\tstartet calculation of maximal partitions");
        }
        long start = System.currentTimeMillis();

        Set<LongList> sortedPartitions = this.sortPartitions(partitions, new ListComparator2());

        if (this.debugSysout) {
            System.out.println("\tTime to sort: " + (System.currentTimeMillis() - start));
        }

        Iterator<LongList> it = sortedPartitions.iterator();
        long remainingPartitions = sortedPartitions.size();
        if (this.debugSysout) {
            System.out.println("\tNumber of Partitions: " + remainingPartitions);
        }

        if (this.optimize()) {
            Map<Long, LongSet> index = new ConcurrentHashMap<Long, LongSet>();
            Map<LongList, Object> max = new ConcurrentHashMap<LongList, Object>();

            long actuelIndex = 0;
            LongList actuelList;

            int currentSize = Integer.MAX_VALUE;
            ExecutorService exec = this.getExecuter();
            while (it.hasNext()) {
                actuelList = it.next();
                if (currentSize != actuelList.size()) {
                    currentSize = actuelList.size();
                    this.awaitExecuter(exec);
                    exec = this.getExecuter();
                }
                exec.execute(new HandlePartitionTask(actuelList, actuelIndex, index, max));
                actuelIndex++;
            }
            this.awaitExecuter(exec);

            long end = System.currentTimeMillis();
            if (this.debugSysout) {
                System.out.println("\tTime needed: " + (end - start));
            }

            index.clear();
            sortedPartitions.clear();

            return max.keySet();
        } else {
            Long2ObjectMap<LongSet> index = new Long2ObjectOpenHashMap<LongSet>();
            Set<LongList> max = new HashSet<LongList>();

            long actuelIndex = 0;
            LongList actuelList;

            while (it.hasNext()) {
                actuelList = it.next();
                this.handlePartition(actuelList, actuelIndex, index, max);
                actuelIndex++;
            }

            long end = System.currentTimeMillis();
            if (this.debugSysout) {
                System.out.println("\tTime needed: " + (end - start));
            }

            index.clear();
            sortedPartitions.clear();

            return max;
        }

    }*/
	
	public static Set<LongList> computeMaximumSets(List<StrippedPartition> partitionsOrig) {

        if (debugSysout) {
            System.out.println("\tstartet calculation of maximal partitions");
        }

        List<StrippedPartition> partitions = new LinkedList<StrippedPartition>();
        for (StrippedPartition p : partitionsOrig) {
            partitions.add(p.copy());
        }

        Set<LongList> maxSets = null;
        for(StrippedPartition sp : partitions) {
        	if(sp.getValues().size() != 0){
        		maxSets = new HashSet<LongList>(sp.getValues());
        		break;
        	}
        }
        
        for (int i = partitions.size() - 2; i >= 0; i--) {
            if (debugSysout)
                System.out.println(i);
            calculateSupersets(maxSets, partitions.get(i).getValues());
        }

        if (debugSysout) {
            long count = 0;
            System.out.println("-----\nAnzahl maximaler Partitionen: " + maxSets.size() + "\n-----");
            for (LongList l : maxSets) {
                System.out.println("Partitionsgröße: " + l.size());
                count = count + (l.size() * (l.size() - 1) / 2);
            }
            System.out.println("-----\nBenötigte Anzahl: " + count + "\n-----");
        }

        return maxSets;
    }
	
	public static void calculateSupersets(Set<LongList> maxSets, List<LongList> partitions) {

        List<LongList> toDelete = new LinkedList<LongList>();
        Set<LongList> toAdd = new HashSet<LongList>();
        int deleteFromPartition = -1;

        // List<LongList> remainingSets = new LinkedList<LongList>();
        // remainingSets.addAll(partition);
        for (LongList maxSet : maxSets) {
            for (LongList partition : partitions) {
                // LongList partitionCopy = new LongArrayList(partition);
                if ((maxSet.size() >= partition.size()) && (maxSet.containsAll(partition))) {
                    toAdd.remove(partition);
                    deleteFromPartition = partitions.indexOf(partition);
                    if (debugSysout)
                        System.out.println("MaxSet schon vorhanden");
                    break;
                }
                if ((partition.size() >= maxSet.size()) && (partition.containsAll(maxSet))) {
                    toDelete.add(maxSet);
                    if (debugSysout)
                        System.out.println("Neues MaxSet");
                }
                toAdd.add(partition);
            }
            if (deleteFromPartition != -1) {
                partitions.remove(deleteFromPartition);
                deleteFromPartition = -1;
            }
        }
        maxSets.removeAll(toDelete);
        maxSets.addAll(toAdd);
    }
	
	public static Long2ObjectMap<TupleEquivalenceClassRelation> calculateRelationships(List<StrippedPartition> partitions) {

        if (debugSysout) {
            System.out.println("\tstartet calculation of relationships");
        }
        Long2ObjectMap<TupleEquivalenceClassRelation> relationships = new Long2ObjectOpenHashMap<TupleEquivalenceClassRelation>();
        for (StrippedPartition p : partitions) {
            calculateRelationship(p, relationships);
        }

        /*System.out.println("=====> Relationships");
        for(long i = 0; i < relationships.size(); i++)
        	System.out.println(relationships.get(i).getRelationships().toString());*/
        
        return relationships;
    }

    private static void calculateRelationship(StrippedPartition partitions, Long2ObjectMap<TupleEquivalenceClassRelation> relationships) {

        int partitionNr = 0;
        for (LongList partition : partitions.getValues()) {
            if (debugSysout)
                System.out.println(".");
            for (long index : partition) {
                if (!relationships.containsKey(index)) {
                    relationships.put(index, new TupleEquivalenceClassRelation());
                }
                relationships.get(index).addNewRelationship(partitions.getAttributeID(), partitionNr);
            }
            partitionNr++;
        }

    }
    
    public static Set<AgreeSet> computeAgreeSets(Long2ObjectMap<TupleEquivalenceClassRelation> relationships, Set<LongList> maxSets,
            List<StrippedPartition> partitions) {
    	if (debugSysout) {
            System.out.println("\tstartet calculation of agree sets");
            int bitsPerSet = (((int) (partitions.size() - 1) / 64) + 1) * 64;
            long setsNeeded = 0;
            for (LongList l : maxSets) {
                setsNeeded += l.size() * (l.size() - 1) / 2;
            }
            System.out
                    .println("Approx. RAM needed to store all agree sets: " + bitsPerSet * setsNeeded / 8 / 1024 / 1024 + " MB");
        }
    	partitions.clear();

        if (debugSysout) {
            System.out.println(maxSets.size());
        }
        int a = 0;

        Set<AgreeSet> agreeSets = new HashSet<AgreeSet>();
        
        for (LongList maxEquiClass : maxSets) {
            if (debugSysout) {
                System.out.println(a++);
            }
            for (int i = 0; i < maxEquiClass.size() - 1; i++) {
                for (int j = i + 1; j < maxEquiClass.size(); j++) {
                    relationships.get(maxEquiClass.getLong(i)).intersectWithAndAddToAgreeSet(
                            relationships.get(maxEquiClass.getLong(j)), agreeSets);
                }
            }
        }

        return agreeSets;
    }
    
    /*public static Set<AgreeSet> computeAgreeSets2(Long2ObjectMap<TupleEquivalenceClassRelation> relationships, Set<LongList> maxSets,
            List<StrippedPartition> partitions) {
    	if (debugSysout) {
            System.out.println("\tstartet calculation of agree sets");
            int bitsPerSet = (((int) (partitions.size() - 1) / 64) + 1) * 64;
            long setsNeeded = 0;
            for (LongList l : maxSets) {
                setsNeeded += l.size() * (l.size() - 1) / 2;
            }
            System.out
                    .println("Approx. RAM needed to store all agree sets: " + bitsPerSet * setsNeeded / 8 / 1024 / 1024 + " MB");
        }
    	partitions.clear();

        if (debugSysout) {
            System.out.println(maxSets.size());
        }
        int a = 0;

        Set<AgreeSet> agreeSets = new HashSet<AgreeSet>();

        for (LongList maxEquiClass : maxSets) {
            if (debugSysout) {
                System.out.println(a++);
            }
            for (int i = 0; i < maxEquiClass.size() - 1; i++) {
            	int[] row1 = table[(new Long(maxEquiClass.getLong(i))).intValue()];
                for (int j = i + 1; j < maxEquiClass.size(); j++) {
                	int[] row2 = table[(new Long(maxEquiClass.getLong(j))).intValue()];
                	AgreeSet set = new AgreeSet();
                	for(int k = 0; k < numberAttributes; k++) {
        				//System.out.print(row1[k] + "-" + row2[k]+" ");
        				if(row1[k] == row2[k])
        					set.add(k);
        			}
                	agreeSets.add(set);
                }
            }
        }
        
        return agreeSets;
    }*/
    
    public static List<DifferenceSet> differenceSetGenerator(List<AgreeSet> agreeSets, int numberOfAttributes) {

    	List<DifferenceSet> returnValue = new LinkedList<DifferenceSet>();
        for (AgreeSet as : agreeSets) {
        	OpenBitSet value = as.getAttributes();

            DifferenceSet ds = new DifferenceSet();
            for (int i = 0; i < numberOfAttributes; i++) {
                if (!value.get(i)) {
                    ds.add(i);
                }
            }
            returnValue.add(ds);
        }

        return returnValue;
    }
    
    public static List<FunctionalDependencyGroup2> findCoversGenerator(List<DifferenceSet> differenceSets, Integer numberOfAttributes) {

        List<FunctionalDependencyGroup2> result = new LinkedList<FunctionalDependencyGroup2>();

        for (int attribute = 0; attribute < numberOfAttributes; attribute++) {

            List<DifferenceSet> tempDiffSet = new LinkedList<DifferenceSet>();

            // Compute DifferenceSet modulo attribute (line 3 - Fig5 - FastFDs)
            for (DifferenceSet ds : differenceSets) {
                OpenBitSet obs = ds.getAttributes().clone();
                if (!obs.get(attribute)) {
                    continue;
                } else {
                    obs.flip(attribute);
                    tempDiffSet.add(new DifferenceSet(obs));
                }
            }

            // check new DifferenceSet (line 4 + 5 - Fig5 - FastFDs)
            if (tempDiffSet.size() == 0) {
            	FunctionalDependencyGroup2 fdg = new FunctionalDependencyGroup2(attribute, new IntArrayList());
            	//fdg.printDependency(columnNames); //this.addFdToReceivers(fdg);
            	fdCount++;
            	writeFDToFile(fdg.toString(), attribute);
            } else if (checkNewSet(tempDiffSet)) {
                List<DifferenceSet> copy = new LinkedList<DifferenceSet>();
                copy.addAll(tempDiffSet);
                doRecusiveCrap(attribute, generateInitialOrdering(tempDiffSet), copy, new IntArrayList(), tempDiffSet,
                        result, attribute);
            }

        }

        return result;
    }
    
    private static boolean checkNewSet(List<DifferenceSet> tempDiffSet) {

        for (DifferenceSet ds : tempDiffSet) {
            if (ds.getAttributes().isEmpty()) {
                return false;
            }
        }

        return true;
    }
    
    private static void doRecusiveCrap(int currentAttribute, IntList currentOrdering, List<DifferenceSet> setsNotCovered,
            IntList currentPath, List<DifferenceSet> originalDiffSet, List<FunctionalDependencyGroup2> result, int attribute) {
    	// Basic Case
        // FIXME
        if (!currentOrdering.isEmpty() && /* BUT */setsNotCovered.isEmpty()) {
            if (debugSysout)
                System.out.println("no FDs here");
            return;
        }

        if (setsNotCovered.isEmpty()) {

            List<OpenBitSet> subSets = generateSubSets(currentPath);
            if (noOneCovers(subSets, originalDiffSet)) {
                FunctionalDependencyGroup2 fdg = new FunctionalDependencyGroup2(currentAttribute, currentPath);
                //fdg.printDependency(columnNames);// this.addFdToReceivers(fdg);
                fdCount++;
                writeFDToFile(fdg.toString(), attribute);
                result.add(fdg);
            } else {
                if (debugSysout) {
                    System.out.println("FD not minimal");
                    System.out.println(new FunctionalDependencyGroup2(currentAttribute, currentPath));
                }
            }

            return;
        }

        // Recusive Case
        for (int i = 0; i < currentOrdering.size(); i++) {

            List<DifferenceSet> next = generateNextNotCovered(currentOrdering.getInt(i), setsNotCovered);
            IntList nextOrdering = generateNextOrdering(next, currentOrdering, currentOrdering.getInt(i));
            IntList currentPathCopy = new IntArrayList(currentPath);
            currentPathCopy.add(currentOrdering.getInt(i));
            doRecusiveCrap(currentAttribute, nextOrdering, next, currentPathCopy, originalDiffSet, result, attribute);
        }
    }
    
    private static List<OpenBitSet> generateSubSets(IntList currentPath) {

        List<OpenBitSet> result = new LinkedList<OpenBitSet>();

        OpenBitSet obs = new OpenBitSet();
        for (int i : currentPath) {
            obs.set(i);
        }

        for (int i : currentPath) {

            OpenBitSet obs_ = obs.clone();
            obs_.flip(i);
            result.add(obs_);

        }

        return result;
    }
    
    private static boolean noOneCovers(List<OpenBitSet> subSets, List<DifferenceSet> originalDiffSet) {

        for (OpenBitSet obs : subSets) {

            if (covers(obs, originalDiffSet)) {
                return false;
            }

        }

        return true;
    }
    
    private static boolean covers(OpenBitSet obs, List<DifferenceSet> originalDiffSet) {

        for (DifferenceSet diff : originalDiffSet) {

            if (OpenBitSet.intersectionCount(obs, diff.getAttributes()) == 0) {
                return false;
            }
        }

        return true;
    }
    
    private static List<DifferenceSet> generateNextNotCovered(int attribute, List<DifferenceSet> setsNotCovered) {

        List<DifferenceSet> result = new LinkedList<DifferenceSet>();

        for (DifferenceSet ds : setsNotCovered) {

            if (!ds.getAttributes().get(attribute)) {
                result.add(ds);
            }
        }

        return result;
    }
    
    private static IntList generateInitialOrdering(List<DifferenceSet> tempDiffSet) {

        IntList result = new IntArrayList();

        Int2IntMap counting = new Int2IntArrayMap();
        for (DifferenceSet ds : tempDiffSet) {

            int lastIndex = ds.getAttributes().nextSetBit(0);

            while (lastIndex != -1) {
                if (!counting.containsKey(lastIndex)) {
                    counting.put(lastIndex, 1);
                } else {
                    counting.put(lastIndex, counting.get(lastIndex) + 1);
                }
                lastIndex = ds.getAttributes().nextSetBit(lastIndex + 1);
            }
        }

        // TODO: Comperator und TreeMap --> Tommy
        while (true) {

            if (counting.size() == 0) {
                break;
            }

            int biggestAttribute = -1;
            int numberOfOcc = 0;
            for (int attr : counting.keySet()) {

                if (biggestAttribute < 0) {
                    biggestAttribute = attr;
                    numberOfOcc = counting.get(attr);
                    continue;
                }

                int tempOcc = counting.get(attr);
                if (tempOcc > numberOfOcc) {
                    numberOfOcc = tempOcc;
                    biggestAttribute = attr;
                } else if (tempOcc == numberOfOcc) {
                    if (biggestAttribute > attr) {
                        biggestAttribute = attr;
                    }
                }
            }

            if (numberOfOcc == 0) {
                break;
            }

            result.add(biggestAttribute);
            counting.remove(biggestAttribute);
        }

        return result;
    }
    
    private static IntList generateNextOrdering(List<DifferenceSet> next, IntList currentOrdering, int attribute) {

        IntList result = new IntArrayList();

        Int2IntMap counting = new Int2IntArrayMap();
        boolean seen = false;
        for (int i = 0; i < currentOrdering.size(); i++) {

            if (!seen) {
                if (currentOrdering.getInt(i) != attribute) {
                    continue;
                } else {
                    seen = true;
                }
            } else {

                counting.put(currentOrdering.getInt(i), 0);
                for (DifferenceSet ds : next) {

                    if (ds.getAttributes().get(currentOrdering.getInt(i))) {
                        counting.put(currentOrdering.getInt(i), counting.get(currentOrdering.getInt(i)) + 1);
                    }
                }
            }
        }

        // TODO: Comperator und TreeMap --> Tommy
        while (true) {

            if (counting.size() == 0) {
                break;
            }

            int biggestAttribute = -1;
            int numberOfOcc = 0;
            for (int attr : counting.keySet()) {

                if (biggestAttribute < 0) {
                    biggestAttribute = attr;
                    numberOfOcc = counting.get(attr);
                    continue;
                }

                int tempOcc = counting.get(attr);
                if (tempOcc > numberOfOcc) {
                    numberOfOcc = tempOcc;
                    biggestAttribute = attr;
                } else if (tempOcc == numberOfOcc) {
                    if (biggestAttribute > attr) {
                        biggestAttribute = attr;
                    }
                }
            }

            if (numberOfOcc == 0) {
                break;
            }

            result.add(biggestAttribute);
            counting.remove(biggestAttribute);
        }

        return result;
    }
    
    public static void writeFDToFile(String fd, int attribute) {
    	System.out.println(fd);
		/*try {
			FileSystem fs = null;
			Path path = new Path("/tmp/fastfd/result-"+attribute);
			fs = FileSystem.get(new Configuration());
			FSDataOutputStream out;
			if(fs.exists(path))
				out = fs.append(path);
			else
				out = fs.create(path);
			BufferedWriter br = new BufferedWriter( new OutputStreamWriter( out) );
			br.write(fd+"\n");
			br.close();
			out.close();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}*/
	}
    
	public static void execute() {

		System.out.println(" ========== Running FastFD =========\n");
        long t1 = System.currentTimeMillis();
        List<StrippedPartition> strippedPartitions = strippedPartitionGenerator();
        long t2 = System.currentTimeMillis();
        System.out.println("strippedPartitionGenerator time(s): " + (t2-t1)/1000);
        
        List<AgreeSet> agreeSets = agreeSetGenerator(strippedPartitions);
        long t3 = System.currentTimeMillis();
        System.out.println("agreeSetGenerator time(s): " + (t3-t2)/1000);
        agreeSets.add(new AgreeSet());

        long t4 = System.currentTimeMillis();
        List<DifferenceSet> diffSets = differenceSetGenerator(agreeSets, numberAttributes);
        long t5 = System.currentTimeMillis();
        System.out.println("differenceSetGenerator time(s): " + (t5-t4)/1000);
        
        findCoversGenerator(diffSets, numberAttributes);
        long t6 = System.currentTimeMillis();
        System.out.println("findCoversGenerator time(s): " + (t6-t5)/1000);
        
        System.out.println("=== TOTAL time(s): " + (t6-t1)/1000);
        System.out.println("=== TOTAL FD COUNT: " + fdCount);

    }
	
    /*public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        String master = args[0];
        
        SparkConf sparkConf = new SparkConf().setAppName("SimpleApp");
		sparkConf.set("spark.kryoserializer.buffer.max.mb", "2048");
		sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
		sparkConf.set("spark.kryo.registrationRequired", "true");
		sc = new JavaSparkContext(master, "Simple App", "$YOUR_SPARK_HOME", new String[]{"target/simple-project-1.0.jar"});
		sc.hadoopConfiguration().set("dfs.replication", "1");
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.setLevel(Level.ERROR);
		
		datasetFile = args[1];
		//mode = Integer.parseInt(args[3]);
		SQLContext sqlContext = new SQLContext(sc);
		
		df = sqlContext.jsonFile(datasetFile);
		df.printSchema();
		columnNames = df.columns();
		numberAttributes = columnNames.length;
		//my_print_args(args);
		//execute();
		distributedEx
		sc.stop();
		
    }*/
}
