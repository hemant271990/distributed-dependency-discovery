package fastdc_helper;

/**
 * Created by Jian on 2016-06-06.
 */

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;

public class Cell {
    // position info
    public int row, column;

    public Object value;
    public Object type;
    public Obj O;
	/*
	 * types in ["Integer", "Double", "String", "Date", null]
	 */
    public Cell(Cell cell) {
        this.type = cell.type;
        this.value = cell.value;
    }

    public Cell(int row, int column, Object type, Object value) {
        this.row = row;
        this.column = column;
        this.value = value;
        this.type = type;
    }

    public Cell(int row, int column, Cell C) {
        this.row = row;
        this.column = column;
        this.value = C.value;
        this.type = C.type;
    }

    public Cell() {
        this.value = null;
        this.row = this.column = -1;
    }

    public boolean isEmpty() {
        return value == null;
    }

    public void setValue(Object v) {
        value = v;
    }

    public Object getValue() {
        return value;
    }

    // point to the same cell or not
    public boolean equals(Cell C) {
        if (C == null) return false;
        if (row != C.row || column != C.column) return false;
        if (C.value == null && value == null) return true;
        if (C.value == null || value == null) return false;
        return (value.equals(C.value));
    }

    // check value and type only
    public boolean EQ(Cell C) {
        if (C == null) return false;
        if (value == null || C.value == null) return false;
        ObjectComparator comparator = new ObjectComparator();
        int k = comparator.compare(value, C.value);
        return k == 0;
    }

    public boolean EQ_obj(Object obj) {
        if (value == null || obj == null) return false;
        ObjectComparator comparator = new ObjectComparator();
        int k = comparator.compare(value, obj);
        return k == 0;
    }

    public boolean IQ(Cell C) {
        if (C == null) return false;
        if (value == null || C.value == null) return false;
        ObjectComparator comparator = new ObjectComparator();
        int k = comparator.compare(value, C.value);
        return k != 0;
    }

    public boolean IQ_obj(Object obj) {
        if (value == null || obj == null) return false;
        ObjectComparator comparator = new ObjectComparator();
        int k = comparator.compare(value, obj);
        return k != 0;
    }

    public boolean GT(Cell C) {
        if (C == null) return false;
        if (value == null || C.value == null) return false;
        ObjectComparator comparator = new ObjectComparator();
        int k = comparator.compare(value, C.value);
        return k > 0;
    }

    public boolean GTE(Cell C) {
        if (C == null) return false;
        if (value == null || C.value == null) return false;
        ObjectComparator comparator = new ObjectComparator();
        int k = comparator.compare(value, C.value);
        return k >= 0;
    }

    public boolean LT(Cell C) {
        if (C == null) return false;
        if (value == null || C.value == null) return false;
        ObjectComparator comparator = new ObjectComparator();
        int k = comparator.compare(value, C.value);
        return k < 0;
    }

    public boolean LTE(Cell C) {
        if (C == null) return false;
        if (value == null || C.value == null) return false;
        ObjectComparator comparator = new ObjectComparator();
        int k = comparator.compare(value, C.value);
        return k <= 0;
    }

    public boolean GT_obj(Object obj) {
        if (value == null || obj == null) return false;
        ObjectComparator comparator = new ObjectComparator();
        int k = comparator.compare(value, obj);
        return k > 0;
    }

    public boolean LT_obj(Object obj) {
        if (value == null || obj == null) return false;
        ObjectComparator comparator = new ObjectComparator();
        int k = comparator.compare(value, obj);
        return k < 0;
    }

    public boolean GTE_obj(Object obj) {
        if (value == null || obj == null) return false;
        ObjectComparator comparator = new ObjectComparator();
        int k = comparator.compare(value, obj);
        return k >= 0;
    }

    public boolean LTE_obj(Object obj) {
        if (value == null || obj == null) return false;
        ObjectComparator comparator = new ObjectComparator();
        int k = comparator.compare(value, obj);
        return k <= 0;
    }

    static public String objectToString(Object v) {
        if (v == null) return "";
        if (v instanceof Integer) return ((Integer)v).toString();
        if (v instanceof Double) return ((Double)v).toString();
        if (v instanceof String) return ((String)v).toString();
        if (v instanceof Date) return ((Date)v).toString();
        return null;
    }

    public String toString(boolean verbose) {
        String str = objectToString(value);
        if (verbose) {
            if (value == null)
                str += " (" + row + "," + column + " " + "null)";
            else
                str += " (" + row + "," + column + " " + value.getClass().getSimpleName() + ")";
        }
        return str;
    }

    public static Object stringToObject(String str, String type, String format) {
        if (str.isEmpty()) return null;
        if (type.equals("Integer")) return new Integer(str);
        if (type.equals("Double")) return new Double(str);
        if (type.equals("String")) return new String(str);
        if (type.equals("Date")) {
			/*
			System.err.println("Currently not support type Date!");
			return null;
			*/
            // support it now
            if (format == null) {
                System.err.println("Cannot find the format of date...");
                return null;
            }
            SimpleDateFormat formatter = new SimpleDateFormat(format);
            Date date;
            try {
                date = formatter.parse(str);
            } catch (ParseException e) {
                System.err.println("Parsing date error: " + str + " with format " + format);
                return null;
            }
            return date;
        }
        System.err.println("Unknown type: " + type);
        return null;
    }

    private static class ObjectComparator implements Comparator<Object> {
    
        @Override
        public int compare(Object o1, Object o2) {
            if (!o1.getClass().equals(o2.getClass()))
                throw new ClassCastException("comparing " + o1.getClass().getName() + " with " + o2.getClass().getName());
    
            if (o1 instanceof Integer) {
                Integer x = (Integer)o1;
                Integer y = (Integer)o2;
                return x.compareTo(y);
            }
            if (o1 instanceof Double) {
                Double x = (Double)o1;
                Double y = (Double)o2;
                return x.compareTo(y);
            }
            if (o1 instanceof String) {
                String x = (String)o1;
                String y = (String)o2;
                return x.compareTo(y);
            }
    
            if (o1 instanceof Date) {
                Date x = (Date)o1;
                Date y = (Date)o2;
                return x.compareTo(y);
            }
    
            throw new ClassCastException("unknown data type");
        }
    
    }
    public Obj getLiteralValue() {
        Object o = value;
        if (o instanceof Date) {
            return new Literal((Date) o);
        }
        if (o instanceof Double) {
            return new Literal((Double) o);
        }
        if (o instanceof Integer) {
            return new Literal((Integer) o);
        }
        return new Literal((String) o);
    }
}
