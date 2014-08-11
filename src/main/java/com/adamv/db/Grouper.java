package com.adamv.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface Grouper<T> {
    T getKey(ResultSet rs) throws SQLException;
}
