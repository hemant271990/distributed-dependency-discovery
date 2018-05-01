package fastdc_helper;

import java.io.Serializable;

/**
 * Created by Jian on 2016-06-07.
 */
public class Triple implements Serializable {
    public URI S, P;
    public Obj O;

    public Triple() {}

    public Triple(URI S, URI P, Obj O) {
        this.S = S;
        this.P = P;
        this.O = O;
    }

    public Triple(Triple T) {
        this.S = T.S;
        this.P = T.P;
        this.O = T.O;
    }

    public String toString() {
        return "S = " + S.toString() + "\n" + "P = " + P.toString() + "\n" + "O = " + O.toString();
    }

    public void print() {
        System.out.println(toString());
    }

    /*
    public static class TripleComparator implements Comparator<Triple> {
        public int compare(Triple x, Triple y) {
            int k = x.S.compareTo(y.S);
            if (k != 0) return k;
            k = x.P.compareTo(y.P);
            if (k != 0) return k;
            return x.O.compareTo(y.O);
        }
    }
    */
}
