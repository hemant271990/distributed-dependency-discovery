package fastdc_helper;

import java.io.Serializable;
import java.util.regex.Pattern;

/**
 * Created by Jian on 2017-03-11.
 */
public class Prefix implements Serializable {
    public final Pattern pattern;
    private final String full;
    public final boolean onlyPrefix;

    public Prefix(String regex, String full, boolean onlyPrefix) {
        this.pattern = Pattern.compile(regex);
        this.full = full;
        this.onlyPrefix = onlyPrefix;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Prefix)) return false;

        Prefix another = (Prefix) o;

        return this.full.equals(another.full);
    }

    @Override
    public int hashCode() {
        return this.full.hashCode();
    }

    @Override
    public String toString() {
        return full;
    }
}
