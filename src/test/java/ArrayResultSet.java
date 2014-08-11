import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * A ResultSet backed by a list of hash maps.
 */
public class ArrayResultSet extends BaseResultSet {
    private final Object[][] data;
    private final List<String> columnNames;

    private long cursor;

    public ArrayResultSet(Object[][] data, String[] columnNames) {
        this.data = data;
        this.columnNames = Arrays.asList(columnNames);
        this.cursor = -1; // start before first list element
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        final int index = columnNames.indexOf(columnLabel);
        if (index == -1) {
            throw new SQLException("No column found for " + columnLabel);
        } else {
            return index;
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (cursor == data.length) {
            return false;
        } else {
            cursor++;
            return cursor != data.length;
        }
    }
}
