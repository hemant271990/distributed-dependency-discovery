package fastfd_helper;

import java.io.Serializable;

import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;


public class FunctionalDependencyGroup2 implements Serializable{

    private int attribute = Integer.MIN_VALUE;
    private IntSet values = new IntArraySet();

    public FunctionalDependencyGroup2(int attributeID, IntList values) {

        this.attribute = attributeID;
        this.values.addAll(values);
    }

    public int getAttributeID() {

        return this.attribute;
    }

    public IntList getValues() {

        IntList returnValue = new IntArrayList();
        returnValue.addAll(this.values);
        return returnValue;

    }

    @Override
    public String toString() {

        return this.values + " --> " + this.attribute;
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + attribute;
        result = prime * result + ((values == null) ? 0 : values.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        FunctionalDependencyGroup2 other = (FunctionalDependencyGroup2) obj;
        if (attribute != other.attribute)
            return false;
        if (values == null) {
            if (other.values != null)
                return false;
        } else if (!values.equals(other.values))
            return false;
        return true;
    }

    public void printDependency(String[] columnNames) {

        for (int i : this.values) {
        	System.out.print(columnNames[i]);
        }
        System.out.print("->");
        System.out.println(columnNames[this.attribute]);
    } 

}

