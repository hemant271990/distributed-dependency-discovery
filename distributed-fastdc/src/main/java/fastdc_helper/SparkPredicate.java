package fastdc_helper;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import fastdc_helper.*;
import fastdc_helper.Obj.ObjectType;
import scala.Serializable;

import java.util.Comparator;

/**
 * Created by Jian on 2017-04-24.
 */
public class SparkPredicate implements Serializable {
    public static final int numericalPredicates = 6;
    public static final int categoricalPredicates = 2;
    public String attrXName, attrYName;
    public boolean isConstant;
    public Object constant;

    public enum OpType {
        EQ(0), NEQ(1), LT(2), LTE(3), GT(4), GTE(5);

        private final int id;
        OpType(int id) {
            this.id = id;
        }
        public int getValue() {
            return id;
        }
    }

    private static final Logger logger = LogManager.getLogger(Predicate.class);

    public Object left, right;
    public OpType op;
    public ObjectType type;
    public int leftIndex, rightIndex;

    public SparkPredicate(OpType op, Object left, Object right, ObjectType type,
                          int leftIndex, int rightIndex) {
        this.op = op;
        this.left = left;
        this.right = right;
        this.type = type;
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
    }

    public SparkPredicate(OpType op, Object left, Object right,
                          int leftIndex, int rightIndex) {
        this.op = op;
        this.left = left;
        this.right = right;
        this.leftIndex = leftIndex;
        this.rightIndex = rightIndex;
    }

    public SparkPredicate negation() {
        return new SparkPredicate(negateOp(op), left, right, type, leftIndex, rightIndex);
    }

    // TODO: make it elegant
    private OpType negateOp(OpType op) {
        if (op == OpType.EQ) return OpType.NEQ;
        if (op == OpType.NEQ) return OpType.EQ;
        if (op == OpType.LT) return OpType.GTE;
        if (op == OpType.LTE) return OpType.GT;
        if (op == OpType.GT) return OpType.LTE;
        if (op == OpType.GTE) return OpType.LT;
        System.out.println("Unknown OpType " + op);
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SparkPredicate)) return false;

        SparkPredicate that = (SparkPredicate)o;

        if (!op.equals(that.op)) return false;
        if (!left.equals(that.left)) return false;
        return right.equals(that.right);
    }

    // TODO: make it elegant
    public static String OpToString(OpType op) {
        if (op == OpType.EQ) return "=";
        if (op == OpType.NEQ) return "!=";
        if (op == OpType.LT) return "<";
        if (op == OpType.LTE) return "<=";
        if (op == OpType.GT) return ">";
        if (op == OpType.GTE) return ">=";
        System.out.println("Unknown OpType: " + op);
        return null;
    }

    // hard code: no constant
    public void appendName(String[] header) {
//        assert(!isConstant);
        attrXName = new String(header[leftIndex]);
        attrYName = new String(header[rightIndex]);
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

    @Override
    public String toString() {
        return "(T1." + left.toString() + " " + OpToString(op) + " T2." + right.toString() + ")";
    }

    public String toString(int numTuples) {
        if (numTuples == 2)
            return "(T1." + left.toString() + " " + OpToString(op) + " T2." + right.toString() + ")";
        return "(T." + left.toString() + " " + OpToString(op) + " T." + right.toString() + ")";
    }

    public String toString(String[] header) {
        String str = "(";

        String xx;
        String yy = null;

        if (header == null) {
            xx = Integer.toString(leftIndex);
            if (rightIndex != -1)
                yy = Integer.toString(rightIndex);
        }
        else {
            xx = header[leftIndex];
            if (rightIndex != -1)
                yy = header[rightIndex];
        }

        if (isConstant)
            str += "T" + "." + xx + " " + OpToString(op) + " \"" + constant + "\"";
        else
            str += "T" + 1 + "." + xx + " " + OpToString(op) + " T" + 2 + "." + yy;
        str += ")";
        return str;
    }
}
