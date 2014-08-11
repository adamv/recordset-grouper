package testutil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A ResultSet backed by an array of arrays and an array of column names.
 */
public class ArrayResultSet extends BaseResultSet {
    private final Object[][] data;
    private final List<String> columnNames;

    private int cursor;

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
            return index + 1; // column indexes are 1-based
        }
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        if (cursorAtEnd()) {
            throw new SQLException("End of data");
        }
        final Object value;
        try {
            value = data[cursor][columnIndex - 1];
        } catch (IndexOutOfBoundsException e) {
            throw new SQLException("columnIndex out of range");
        }
        try {
            return (String) value;
        } catch (ClassCastException e) {
            throw new SQLException("Column is not of type string");
        }
    }

    @Override
    public boolean next() throws SQLException {
        if (cursorAtEnd()) {
            return false;
        } else {
            cursor++;
            return !cursorAtEnd();
        }
    }

    public String describeRecord() {
        if (cursor == -1) {
            return "<at beginning>";
        }

        if (cursorAtEnd()) {
            return "<at end>";
        }

        String s = Objects.toString(data[cursor][0]);

        for (int i = 1; i < data[cursor].length; i++) {
            s += ", ";
            s += Objects.toString(data[cursor][i]);
        }

        return s;
    }

    private boolean cursorAtEnd() {
        return cursor == data.length;
    }

    public static ArrayResultSet createEmpty(String[] columnNames) {
        return new ArrayResultSet(new Object[][]{}, columnNames);
    }
}
