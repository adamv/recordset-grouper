package com.adamv.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class Grouper<T> {
    public abstract T getKey(ResultSet rs) throws SQLException;
}
