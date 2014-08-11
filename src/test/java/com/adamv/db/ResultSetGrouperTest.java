package com.adamv.db;

import org.junit.Test;
import testutil.ArrayResultSet;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;

public class ResultSetGrouperTest {

    @Test
    public void emptyRecordSetHasNoGroups() throws Exception {
        ArrayResultSet rs = ArrayResultSet.createEmpty(new String[]{"header1"});

        ResultSetGrouper g = new ResultSetGrouper<>(rs, new FirstColumnGrouper());
        assertFalse(g.nextGroup());
    }

    @Test(expected = IllegalStateException.class)
    public void newGroupMustBeCalledBeforeNextInGroup() throws Exception {
        ResultSet rs = sampleRs();
        ResultSetGrouper g = new ResultSetGrouper<>(rs, new FirstColumnGrouper());

        g.nextInGroup();
    }

    @Test
    public void navigateAllGroupsAndRecordsInSample() throws Exception {
        ArrayResultSet rs = sampleRs();
        ResultSetGrouper g = new ResultSetGrouper<>(rs, new FirstColumnGrouper());

        assertTrue(g.nextGroup());
        assertEquals("a", g.groupKey());
        assertTrue(g.nextInGroup());
        System.out.println(rs.describeRecord() + " : " + g.groupKey());
        assertTrue(g.nextInGroup());
        System.out.println(rs.describeRecord() + " : " + g.groupKey());
        assertFalse(g.nextInGroup());

        assertTrue(g.nextGroup());
        assertEquals("b", g.groupKey());
        assertTrue(g.nextInGroup());
        System.out.println(rs.describeRecord() + " : " + g.groupKey());
        assertTrue(g.nextInGroup());
        System.out.println(rs.describeRecord() + " : " + g.groupKey());
        assertFalse(g.nextInGroup());

        assertTrue(g.nextGroup());
        assertEquals("c", g.groupKey());
        assertTrue(g.nextInGroup());
        System.out.println(rs.describeRecord() + " : " + g.groupKey());
        assertTrue(g.nextInGroup());
        System.out.println(rs.describeRecord() + " : " + g.groupKey());
        assertFalse(g.nextInGroup());

        assertFalse(g.nextGroup());
    }

    private static class FirstColumnGrouper implements Grouper<String> {
        @Override
        public String getKey(ResultSet rs) throws SQLException {
            return rs.getString(1);
        }
    }

    private static ArrayResultSet sampleRs() {
        return new ArrayResultSet(
          new Object[][]{
              // group one
              {"a", "b", "c"},
              {"a", "d", "e"},
              // group two
              {"b", "b", "c"},
              {"b", "d", "e"},
              // group three
              {"c", "b", "c"},
              {"c", "d", "e"},
          },
          new String[] {"one", "two", "three"}
        );
    }
}
