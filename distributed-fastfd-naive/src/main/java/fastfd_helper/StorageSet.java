package fastfd_helper;

import java.io.Serializable;

public abstract class StorageSet implements Serializable{

    @Override
    public String toString() {

        return this.toString_();
    }

    protected abstract String toString_();
}
