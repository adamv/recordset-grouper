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
    private final Grouper<T> g;

    private boolean started;
    private boolean finished;
    private boolean consumed;
    private T key;

    public ResultSetGrouper(ResultSet rs, Grouper<T> g) {
        this.rs = rs;
        this.started = false;
        this.finished = false;
        this.key = null;
        this.g = g;
    }

    public boolean nextGroup() throws SQLException {
        // If we have finished advancing, stay finished.
        if (finished) return false;

        // If we have not started, advance to the first record in the
        // ResultSet, handling the empty case.
        // Otherwise, nextInGroup will have already advanced the ResultSet.
        if (!started) {
            started = true;

            final boolean b = rs.next();
            if (!b) {
                finished = true;
                return false;
            }
            consumed = false;
        }

        key = g.getKey(rs);

        return true;
    }

    public boolean nextInGroup() throws SQLException {
        // If we have finished advancing, stay finished
        if (finished) return false;

        // It is an error to advance within a group if nextGroup has not been
        // called once
        if (!started) throw new IllegalStateException("nextGroup must be called first");

        if (!consumed) {
            consumed = true;
            return true;
        }


    }

    public T groupKey() {
        if (!started) throw new IllegalStateException("Iteration has not started");
        return key;
    }

    public abstract class Grouper<T> {
        public abstract T getKey(ResultSet rs);
    }
}
