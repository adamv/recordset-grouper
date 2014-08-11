package com.adamv.db;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Provides a grouping operator over sorted result sets.
 *
 * Given a ResultSet ordered by column A, the grouper allows
 * clients to consume each sequence run of identical A without
 * having to keep track of the last seen record or handle
 * the last record as a special case.
 *
 */
public class ResultSetGrouper<T> {
    private final ResultSet rs;
    private final Grouping<T> grouping;

    private boolean started;
    private boolean finished;
    private boolean consumed;
    private boolean newGroupPending;
    private T key;

    public ResultSetGrouper(ResultSet rs, Grouping<T> grouping) {
        this.rs = rs;
        this.started = false;
        this.finished = false;
        this.newGroupPending = false;
        this.key = null;
        this.grouping = grouping;
    }

    public boolean nextGroup() throws SQLException {
        // If we have finished advancing, stay finished.
        if (finished) return false;

        if (started && !newGroupPending) {
            throw new IllegalStateException("Existing group has not been exhausted");
        }

        // If we have not started, advance to the first record in the
        // ResultSet, handling the empty case.
        // Otherwise, nextInGroup will have already advanced the ResultSet.
        if (!started) {
            started = true;

            final boolean moreRecordsInRs = rs.next();
            if (!moreRecordsInRs) {
                finished = true;
                return false;
            }
            consumed = false;
        }

        key = grouping.getKey(rs);
        if (newGroupPending) {
            newGroupPending = false;
            consumed = false;
        }

        return true;
    }

    public boolean nextInGroup() throws SQLException {
        // If we have finished advancing, stay finished
        if (finished) return false;

        // It is an error to advance within a group if nextGroup has not been
        // called once
        if (!started) throw new IllegalStateException("nextGroup must be called first");

        // It is illegal to call nextInGroup again after it has returned false once
        if (newGroupPending) throw new IllegalStateException("This group has been exhausted");

        // Consume the record advanced-to from nextGroup, if there is one.
        if (!consumed) {
            consumed = true;
            return true;
        }

        // Advance to the next record and check if it is still in the same group
        final boolean moreRecordsInRs = rs.next();

        // If out of records, stop iteration
        if (!moreRecordsInRs) {
            finished = true;
            return false;
        }

        final T thisKey = grouping.getKey(rs);
        // If thisKey is the same as key, we are still in the same group.
        if ((thisKey == null && key == null) || (key.equals(thisKey))) {
            return true;
        } else {
            // otherwise, we are looking at a new group
            newGroupPending = true;
            return false;
        }
    }

    public T groupKey() {
        if (!started) throw new IllegalStateException("Iteration has not started");
        if (finished) throw new IllegalStateException("Iteration has finished");
        return key;
    }
}
