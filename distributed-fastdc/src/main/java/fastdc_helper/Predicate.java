package fastdc_helper;

/**
 * Created by Jian on 2016-06-07.
 */

/*
 * OP(t<x>.<attribute X>,t<y>.<attribute Y>)
 * where OP in ["EQ", "IQ", "GT", "LT", "GTE", "LTE"]
 */

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

public class Predicate implements Serializable{
    public String op;

    public int tX, tY;
    public int attrX, attrY;

    public boolean isConstant;
    public Object constant;

    public String attrXName, attrYName;

    //private static final String formatDate = "";

    public static final int numericalPredicates = 6;
    public static final int categoricalPredicates = 2;

    private static final Logger logger = LogManager.getLogger(Predicate.class);

    public void remapping(List<Integer> columnMapping) {
        attrX = columnMapping.get(attrX);
        if (!isConstant)
            attrY = columnMapping.get(attrY);
    }

    /*
    public Predicate(Predicate p) {
        this.op = p.op;
        this.tX = p.tX;
        this.tY = p.tY;
        this.attrX = p.attrX;
        this.attrY = p.attrY;
        this.isConstant = p.isConstant;
        this.constant = p.constant;
        this.attrXName = p.attrXName;
        this.attrYName = p.attrYName;
    }
    */

    public Predicate(String op, int tX, int attrX, int tY, int attrY) {
        this.op = op;
        this.tX = tX;
        this.tY = tY;
        this.attrX = attrX;
        this.attrY = attrY;
        this.isConstant = false;
        this.constant = null;
        this.attrXName = this.attrYName = null;
    }

    public Predicate(String op, int t, int attr, Object constant) {
        this.op = op;
        this.tX = t;
        this.tY = -1;
        this.attrX = attr;
        this.attrY = -1;

        this.isConstant = true;
        this.constant = constant;

        this.attrXName = this.attrYName = null;
    }

    public static int offsetPredicate(String type) {
        if (type.equals("String")) return categoricalPredicates;
        if (type.equals("Integer") || type.equals("Double") || type.equals("Date")) return numericalPredicates;
        System.out.println("Offset Predicate: unknown type " + type);
        return -1;
    }

    // hard code: no constant
    public void appendName(String[] header) {
        assert(!isConstant);
        attrXName = new String(header[attrX]);
        attrYName = new String(header[attrY]);
    }

    // hard code: no constant, attrX == attrY
    public boolean equals(Predicate p) {
        return (this.op.equals(p.op) && this.attrXName.equals(p.attrXName));
    }

    public static String negateOP(String op) {
        if (op.equals("=")) return("!=");
        if (op.equals("!=")) return("=");
        if (op.equals(">")) return("<=");
        if (op.equals("<")) return(">=");
        if (op.equals(">=")) return("<");
        if (op.equals("<=")) return(">");
        System.out.println("negateOP, Unknown op: " + op);
        return null;
    }

    public Predicate negation() {
        String nop = negateOP(op);
        if (isConstant)
            return new Predicate(nop, tX, attrX, constant);
        else
            return new Predicate(nop, tX, attrX, tY, attrY);
    }

    /*
    public String toString(String[] header) {
        String str = "(" + op + ", ";

        String xx = "";
        String yy = "";

        if (header == null) {
            xx = Integer.toString(attrX);
            if (attrY != -1)
                yy = Integer.toString(attrY);
        }
        else {
            xx = header[attrX];
            if (attrY != -1)
                yy = header[attrY];
        }

        if (isConstant)
            str += "T" + tX + "." + xx + ", \"" + constant + "\"";
        else
            str += "T" + tX + "." + xx + ", T" + tY + "." + yy;
        str += ")";
        return str;
    }
    */

    public String toString() {
        assert (attrXName != null && attrYName != null);
        return "(T" + tX + "." + attrXName + " " + op + " T" + tY + "." + attrYName + ")";
    }

    public String toString(String[] header) {
        String str = "(";

        String xx;
        String yy = null;

        if (header == null) {
            xx = Integer.toString(attrX);
            if (attrY != -1)
                yy = Integer.toString(attrY);
        }
        else {
            xx = header[attrX];
            if (attrY != -1)
                yy = header[attrY];
        }

        if (isConstant)
            str += "T" + "." + xx + " " + op + " \"" + constant + "\"";
        else
            str += "T" + tX + "." + xx + " " + op + " T" + tY + "." + yy;
        str += ")";
        return str;
    }

    /*
    public void printInvolvedCellsIndex() {
        System.out.println("(" + tX + ", " + attrX + ")");
        if (!isConstant)
            System.out.println("(" + tY + ", " + attrY + ")");
    }
    */

    public static class PredicateComparatorA implements Comparator<Predicate> {

        @Override
        /*
         * Order follows by:
         * 		operation == "EQ"
         * 		isConstant
         * 		attrX == attrY
         */
        public int compare(Predicate x, Predicate y) {
            if (x.op.equals("EQ") && y.op.equals("EQ")) {
                if (x.isConstant && y.isConstant) return 0;
                if (x.isConstant) return -1;
                if (y.isConstant) return 1;
                if (x.attrX == x.attrY && y.attrX == y.attrY) return 0;
                if (x.attrX == x.attrY) return -1;
                if (y.attrX == y.attrY) return 1;
                return 0;
            }
            if (x.op.equals("EQ")) return -1;
            if (y.op.equals("EQ")) return 1;
            return 0;
        }

    }

    public static class PredicateComparatorB implements Comparator<Predicate> {

        @Override
        /*
         * Order follows by:
         * 		operation
         *      attrXName (i.e. attrYName)
         */
        // hard code: no constant, assume two tuples, attrX == attrY
        public int compare(Predicate x, Predicate y) {
            int k = x.op.compareTo(y.op);
            if (k != 0) return k;
            return x.attrXName.compareTo(y.attrXName);
        }

    }
}
