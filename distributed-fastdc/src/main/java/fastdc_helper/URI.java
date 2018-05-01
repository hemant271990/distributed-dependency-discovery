package fastdc_helper;

import java.io.Serializable;
import java.util.List;
import java.util.regex.Matcher;

/**
 * Created by Jian on 2017-03-11.
 */
public class URI extends Obj implements Serializable {
    private Prefix prefix;
    private String identifier;

    public URI(Prefix prefix, String identifier) {
        super(ObjectType.URI);
        this.prefix = prefix;
        this.identifier = identifier;
    }

    public URI(String uri, List<Prefix> prefixes) {
        super(ObjectType.URI);
        if (prefixes != null) {
            for (Prefix p : prefixes) {
                Matcher m = p.pattern.matcher(uri);
                if (m.find()) {
                    this.prefix = p;
                    if (!p.onlyPrefix)
                        this.identifier = m.group(1);
                    return;
                }
            }
        }
        //System.out.println("Cannot compress: " + uri);
        this.identifier = uri;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof URI)) {
            return false;
        }

        URI uri = (URI) o;

        return (prefix != null ? prefix.equals(uri.prefix) : uri.prefix == null) && (
            identifier != null ? identifier.equals(uri.identifier) : uri.identifier == null);
    }

    @Override
    public int hashCode() {
        int result = prefix != null ? prefix.hashCode() : 0;
        result = 31 * result + (identifier != null ? identifier.hashCode() : 0);
        return result;
    }

    public void print() {
        System.out.println(toString());
    }

    @Override
    public String toString() {
        if (prefix == null)
            return identifier;
        if (identifier == null)
            return prefix.toString();
        return prefix.toString() + identifier;
    }
}

