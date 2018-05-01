package fastdc_helper;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.Date;

public class Literal extends Obj implements Comparable<Literal>, Serializable {
    public Object value;

    private static final Logger logger = LogManager.getLogger(Literal.class);

    public Literal(Integer value) {
        super(ObjectType.IntegerLiteral);
        this.value = value;
    }

    public Literal(Double value) {
        super(ObjectType.DoubleLiteral);
        this.value = value;
    }

    public Literal(String value) {
        super(ObjectType.StringLiteral);
        this.value = value;
    }

    public Literal(Date value) {
        super(ObjectType.DateLiteral);
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Literal)) return false;

        return compareTo((Literal)o) == 0;
    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    public int compareTo(Literal o) {
        // TODO: currently it's hard code here.
        assert type == o.type;
        if (type == ObjectType.IntegerLiteral) {
            Integer A = (Integer)value;
            Integer B = (Integer)o.value;
            return A.compareTo(B);
        } else if (type == ObjectType.DoubleLiteral) {
            Double A = (Double)value;
            Double B = (Double)o.value;
            return A.compareTo(B);
        } else if (type == ObjectType.StringLiteral) {
            String A = (String)value;
            String B = (String)o.value;
            return A.compareTo(B);
        } else if (type == ObjectType.DateLiteral) {
            Date A = (Date)value;
            Date B = (Date)o.value;
            return A.compareTo(B);
        } else {
            System.out.println("ERROR: Unsupported Literal Type " + type);
        }
        return -100;
    }

    public String toString() {
        return value.toString() + "(" + type + ")";
    }
}
