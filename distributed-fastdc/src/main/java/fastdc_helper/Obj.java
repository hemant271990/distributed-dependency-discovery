package fastdc_helper;

import java.io.Serializable;


public class Obj implements Serializable {
    public enum ObjectType {
        URI(0, 2), StringLiteral(1, 2), IntegerLiteral(2, 6), DoubleLiteral(3, 6), DateLiteral(4, 6);

        private final int id, predicateSpaceSize;
        ObjectType(int id, int predicateSpaceSize) {
            this.id = id;
            this.predicateSpaceSize = predicateSpaceSize;
        }

        public int getValue() {
            return id;
        }

        public int getPredicateSpaceSize() {
            return predicateSpaceSize;
        }
    }

    public ObjectType type;

    public Obj(ObjectType type) {
        this.type = type;
    }
}
