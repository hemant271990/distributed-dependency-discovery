package fastod_helper;

import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectBigArrayBigList;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class StrippedPartition implements Serializable {
    private double error;
    private long elementCount;
    private ObjectBigArrayBigList<LongBigArrayBigList> strippedPartition = null;
    //public static boolean ReverseRankingTrue = false;
    //public static boolean RankDoubleTrue = true;
    //public static boolean reverseRank = true;
    
    public StrippedPartition() {}
    /**
     * Create a StrippedPartition with only one equivalence class with the definied number of elements. <br/>
     * Tuple ids start with 0 to numberOfElements-1
     *
     * @param numberTuples
     */
    public StrippedPartition(long numberTuples) {
        this.strippedPartition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        this.elementCount = numberTuples;
        // StrippedPartition only contains partition with more than one elements.
        if (numberTuples > 1) {
            LongBigArrayBigList newEqClass = new LongBigArrayBigList();
            for (int i = 0; i < numberTuples; i++) {
                newEqClass.add(i);
            }
            this.strippedPartition.add(newEqClass);
        }
        this.calculateError();
    }

    /**
     * Create a StrippedPartition from a HashMap mapping the values to the tuple ids.
     *
     * @param partition
     */
    public StrippedPartition(Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition) {
        this.strippedPartition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        this.elementCount = 0;

        //create stripped partitions -> only use equivalence classes with size > 1.
        for (LongBigArrayBigList eqClass : partition.values()) {
            if (eqClass.size64() > 1) {
                strippedPartition.add(eqClass);
                elementCount += eqClass.size64();
            }
        }
        this.calculateError();
    }

    //

    /**
     * Create a StrippedPartition from a HashMap mapping the values to the tuple ids.
     *
     * @param partition
     * @param TAU_SortedList
     */
    public StrippedPartition(
            Object2ObjectOpenHashMap<Object, LongBigArrayBigList> partition,
            ArrayList<ObjectBigArrayBigList<LongBigArrayBigList>> TAU_SortedList,
            ArrayList<ObjectBigArrayBigList<Integer>> attributeValuesList,
            long numberTuples) {

        this.strippedPartition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        this.elementCount = 0;

        ObjectBigArrayBigList<LongBigArrayBigList> TAU_singleList = new ObjectBigArrayBigList<LongBigArrayBigList>();
        ObjectBigArrayBigList<Integer> attributeSingleList = new ObjectBigArrayBigList<Integer>();



        //OD
        ArrayList<Object> partitionKeyList = new ArrayList<Object>(partition.keySet());

        Collections.sort(partitionKeyList, new Comparator<Object>() {
            @Override
            public int compare(Object o1, Object o2)
            {
            	boolean ReverseRankingTrue = false;
            	boolean RankDoubleTrue = true;
            	boolean reverseRank = true;
                if(!ReverseRankingTrue) {

                    boolean isDouble = true;
                    try {
                        double d1 = Double.parseDouble(o1.toString().trim());
                        double d2 = Double.parseDouble(o2.toString().trim());
                    } catch (Exception ex) {
                        isDouble = false;
                    }

                    if (isDouble && RankDoubleTrue) {

                        double d1 = Double.parseDouble(o1.toString().trim());
                        double d2 = Double.parseDouble(o2.toString().trim());

                        if (d1 == d2)
                            return 0;
                        if (d1 > d2)
                            return 1;
                        else
                            return -1;

                    } else {
                        String s1 = o1.toString();
                        String s2 = o2.toString();
                        return s1.compareTo(s2);
                    }

                }else{

                    //ReverseRankingTrue is TRUE

                    boolean isDouble = true;
                    try {
                        double d1 = Double.parseDouble(o1.toString().trim());
                        double d2 = Double.parseDouble(o2.toString().trim());
                    } catch (Exception ex) {
                        isDouble = false;
                    }

                    if (isDouble && RankDoubleTrue) {

                        double d1 = Double.parseDouble(o1.toString().trim());
                        double d2 = Double.parseDouble(o2.toString().trim());

                        //NOTE: we only reverse the string rankings, the double/integer ranking stays the same
                        if (d1 == d2)
                            return 0;
                        if (d1 > d2)
                            return 1;
                        else
                            return -1;

                    } else {
                        String s1 = o1.toString();
                        String s2 = o2.toString();

                        if(reverseRank) {
                            return -s1.compareTo(s2); //reverse
                        }else{
                            return s1.compareTo(s2); //regular
                        }
                    }

                }
            }
        });

        //create stripped partitions -> only use equivalence classes with size > 1.

        for(int i = 0; i<numberTuples; i ++){
            attributeSingleList.add(-1);
        }

        //OD
        for(int i = 0; i<partitionKeyList.size(); i ++) {
            LongBigArrayBigList eqClass = partition.get(partitionKeyList.get(i));

            if (eqClass.size64() > 1) {
                strippedPartition.add(eqClass);
                elementCount += eqClass.size64();
            }

            //for TAU, we need all eqClasses, including size 1
            TAU_singleList.add(eqClass);

            //add elements to attributeSingleList
            for(long elementId : eqClass){
                attributeSingleList.set(elementId, i);
                //attributeSingleList.set(elementId, Integer.parseInt(partitionKeyList.get(i).toString()));
            }
        }

        TAU_SortedList.add(TAU_singleList);
        attributeValuesList.add(attributeSingleList);

        this.calculateError();
    }



    public StrippedPartition(ObjectBigArrayBigList<LongBigArrayBigList> sp, long elementCount) {
        this.strippedPartition = sp;
        this.elementCount = elementCount;
        this.calculateError();

    }

    public double getError() {
        return error;
    }
    
    public long getElementCount() {
        return elementCount;
    }

    public ObjectBigArrayBigList<LongBigArrayBigList> getStrippedPartition() {
        return this.strippedPartition;
    }

    private void calculateError() {
        // calculating the error. Dividing by the number of entries
        // in the whole population is not necessary.
        this.error = this.elementCount - this.strippedPartition.size64();
    }

    public void empty() {
        this.strippedPartition = new ObjectBigArrayBigList<LongBigArrayBigList>();
        this.elementCount = 0;
        this.error = 0.0;
    }
}
