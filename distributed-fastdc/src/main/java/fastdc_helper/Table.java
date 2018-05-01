package fastdc_helper;

/**
 * Created by Jian on 2016-06-06.
 */

import java.util.*;

public class Table {
    public int row, column;

    public Cell[][] table;

    public String[] header;

    // Attribute Name --> Column Number
    public Map<String, Integer> lookup;

    public String[] types;

	/*
	 * types in ["Integer", "Double", "String", "Date", null]
	 */

    public Set<Object> domainOfColumn(int column) {
        Set<Object> stats = new HashSet<Object>();

        for (int i = 0; i < row; ++ i) {
            Object key = table[i][column].value;
            stats.add(key);
        }

        return stats;
    }

    public Map<Object, BitSet> domainOfColumnWithTupleIndices(int column) {
        Map<Object, BitSet> stats = new HashMap<Object, BitSet>();

        for (int i = 0; i < row; ++ i) {
            Object key = table[i][column].value;
            if (stats.containsKey(key)) {
                BitSet T = stats.get(key);
                T.set(i);
            }
            else {
                BitSet T = new BitSet(row);
                T.set(i);
                stats.put(key, T);
            }
        }

        return stats;
    }

    private void buildLookUp() {
        lookup = new HashMap<String, Integer>();
        for (int i = 0; i < column; i ++) {
            lookup.put(header[i], i);
        }
    }

    public Table() {
        row = column = 0;
        table = null;
        header = null;
        types = null;

        lookup = null;
    }

    // construct empty table
    public Table(int row, int column) {
        this.row = row;
        this.column = column;
        this.table = new Cell[row][column];
        this.header = new String[column];
        this.types = new String[column];

        this.lookup = new HashMap<String, Integer>();
    }

    // deep copy an exist table
    /*
    public Table(Table T) {
        this.row = T.row;
        this.column = T.column;

        // copy table
        this.table = new Cell[T.row][T.column];
        for (int i = 0; i < T.row; i ++) {
            for (int j = 0; j < T.column; j ++)
                this.table[i][j] = new Cell(T.table[i][j]);
        }

        // copy header
        this.header = new String[T.column];
        for (int i = 0; i < T.column; i ++)
            this.header[i] = new String(T.header[i]);

        // copy types
        this.types = new String[T.column];
        for (int i = 0; i < T.column; i ++)
            this.types[i] = new String(T.types[i]);

        buildLookUp();
    }
    */

    public Table(int row, int column, Cell[][] T, String[] H, String[] Types) {
        this.row = row;
        this.column = column;

        this.table = T;
        this.header = H;
        this.types = Types;
		/*
		this.table = new Cell[row][column];
		for (int i = 0; i < row; i ++) {
			for (int j = 0; j < column; j ++) {
				if (T[i][j] == null)
					System.out.println(i + " " + j + " (null)");
				else
					System.out.println(i + " " + j + " " + T[i][j].toString(true));
				this.table[i][j] = new Cell(T[i][j]);
			}
		}

		this.header = new String[column];
		for (int i = 0; i < column; i ++)
			this.header[i] = new String(H[i]);

		this.types = new String[column];
		for (int i = 0; i < column; i ++)
			this.types[i] = new String(Types[i]);
		*/

        buildLookUp();
    }

    public void print() {
        for (int i = 0; i < column; i ++) {
            System.out.print(header[i] + "(" + types[i] + ")" + "\t");
        }
        System.out.println();

        for (int i = 0; i < row; i ++) {
            for (int j = 0; j < column; j ++) {
                if (table[i][j].isEmpty())
                    System.out.print("(null)");
                else
                    System.out.print(table[i][j].toString(false));
                System.out.print("\t");
            }
            System.out.println();
        }
    }

    public void printSchema(){
    if(header != null){
        int i=0;
        for (String s: header
             ) {
            System.out.println(s+" --> "+types[i++]);
        }
    }
    }

}

