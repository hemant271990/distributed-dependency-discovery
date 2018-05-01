package fastdc_helper;

import java.util.List;

/**
 * Created by Jian on 2016-08-04.
 */
public class SubTable extends Table {
    public List<Integer> columnMapping;

    public SubTable(int row, int column, Cell[][] T, String[] H, String[] Types, List<Integer> columnMapping) {
        super(row, column, T, H, Types);
        this.columnMapping = columnMapping;
    }
}
