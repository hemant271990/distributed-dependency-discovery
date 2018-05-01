package fastdc_helper;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Serializable;
import scala.Tuple2;

import java.util.*;

import static java.lang.StrictMath.toIntExact;

public class Sparkdf implements Serializable {

    // DataFrame
    public Dataset df;

    public int numP;
    public long numS;
    public long numSchema;
    String[] datatype;
    String[] H;
    public List<String> allP;
    public Map<String, Obj.ObjectType> allPTypes;
    public Map<String, String> types;
    //public Table table;
    public JavaPairRDD<Integer, Iterable<Cell>> tableRDD;
    //public List<Tuple2<Integer,List<Cell>>> tuples;
    public String[] columnNames;

    //    private Map<URI, Integer> indexP;
    public Map<String, Integer> indexP;

    public Sparkdf(Dataset df) {
        this.df = df;
        this.numP = df.columns().length;
        this.numS = df.count();
        this.numSchema = 1;
        this.columnNames = df.columns();
    }

    public void analyze() {
        System.out.println("INFO: Analyzing DataFrame ...");
        datatype = new String[numP];
        H = new String[numP];
        types = new HashMap<>();
        allP = new ArrayList<>();
        allPTypes = new HashMap<>();
        String p;
        String type;
        int o,c; //open '(' and close ')'
        int i =0;
        for (int j = 0; j < columnNames.length; j++){
        	String s = columnNames[j];
            try {
                //System.out.println(s);
                o = s.indexOf("(");
                c = s.indexOf(")");
                p = s.substring(0,o);
                type= s.substring(o+1,c);
            }
            catch (Exception e){
                p = s;
                type= df.schema().fields()[i].dataType().typeName();
                //System.out.println(type);
            }
            allP.add(p);
            H[i] = p;
            switch (type.toLowerCase()){
                case "string":
                    allPTypes.put(p, Obj.ObjectType.StringLiteral);
                    types.put(p, "string");
                    datatype[i++] = "string";
                    break;
                case "integer":
                    allPTypes.put(p, Obj.ObjectType.IntegerLiteral);
                    types.put(p, "integer");
                    datatype[i++] = "integer";
                    break;
                case "long": // HEMANT: treat long same as integers
                    allPTypes.put(p, Obj.ObjectType.IntegerLiteral);
                    types.put(p, "integer");
                    datatype[i++] = "integer";
                    break;
                case "double":
                    allPTypes.put(p, Obj.ObjectType.DoubleLiteral);
                    types.put(p, "double");
                    datatype[i++] = "double";
                    break;
                case "date":
                    allPTypes.put(p, Obj.ObjectType.DateLiteral);
                    types.put(p, "date");
                    datatype[i++] = "date";
                    break;
                default:
                	allPTypes.put(p, Obj.ObjectType.StringLiteral);
                    types.put(p, "string");
                    datatype[i++] = "string";

            }
        }

        System.out.println("INFO: Counting all subjects and all properties...");

		for (int k = 0; k < allP.size(); k++) {
        	String property = allP.get(k);
            System.out.println("INFO: "+property+" --> "+ allPTypes.get(property));
        }

        System.out.println("INFO: # Subjects = "+ numS);
        System.out.println("INFO: # Properties = "+ numP);

        indexP = new HashMap<>();
        int index = 0;
        for (String s : allP) {
            indexP.put(s, index);
            ++ index;
        }
    }
    public List<Tuple2<Integer, Iterable<Cell>>> toRDD(){
        Cell[][] T = new Cell[toIntExact(numS)][numP];
        List<Row> data = df.collectAsList();
        int row =0;
        int col=0;
        List<Tuple2<Integer, Iterable<Cell>>> tuples = new ArrayList<Tuple2<Integer, Iterable<Cell>>>();
        
        for(Row r: data){
            //String[] s = r.toString().split(",");
            List<Cell> tuple = new ArrayList<>();
            //for(String c: s){
            for(int col2 = 0; col2 < numP; col2++) {
            	String c = r.get(col2) != null ? r.get(col2).toString() : "";
                T[row][col] = new Cell(row,col,types.get(H[col]),c);
                tuple.add(col, new Cell(row,col,types.get(H[col]),c));
				//System.out.print(types.get(H[col])+":"+c+" ");
                col++;
            }
			//System.out.println();
            tuples.add(new Tuple2(row,tuple));
            row++;
            col=0;
        }
        return tuples;
        //table = new Table(toIntExact(numS), numP, T, H , datatype);
        //return table;
    }

}
