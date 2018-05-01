package fastdc_helper;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Pipeline {

    public  <Any> Any readDataset (JavaSparkContext jsc, String datasetPath, String format){
        // Read lines
        JavaRDD<String> lines = jsc.textFile(datasetPath);

        switch (format.toLowerCase()) {
            case "csv":
                System.out.println("INFO: Using CSV parser to read file: " + datasetPath);
                String s =  lines.first().toString();
//                ColumnMapping colMap = new ColumnMapping(s);
//                tuples = lines.map(line -> CSV.parseLine(line,colMap )).filter(tuple -> tuple != null);
                break;

            default:
                System.out.println("ERROR: Cannot parse parser of type: " + format);
                throw new IllegalArgumentException("Unknown triples parser: " + format);
        }
        if(format.equalsIgnoreCase("CSV")) {
//            System.out.println("Count: "+tuples.count());
            return (Any) Object.class; //return tuples or table data
        }
        else
            return (Any) Object.class; //triples;

    }

    public Table listtoTable(){

        return null;
    }
}
