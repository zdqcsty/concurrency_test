package com.example.concurrency_test.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RunSql implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(RunSql.class);

    private String driverName = "org.apache.hive.jdbc.HiveDriver";

    private String CONNECTION_URL;

    private String user;

    private String password;

    public AtomicReference<Exception> ex;

    public String sql;

    public Semaphore sem;

    public RunSql(AtomicReference<Exception> ex, Semaphore sem, String sql, String jdbcUrl, String user, String password) {
        this.ex = ex;
        this.sql = sql;
        this.sem = sem;
        this.CONNECTION_URL = jdbcUrl;
        this.user = user;
        this.password = password;
    }

    public Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName(this.driverName);
        } catch (ClassNotFoundException e) {
            System.exit(1);
        }
        try {
            connection = DriverManager.getConnection(this.CONNECTION_URL, this.user, this.password);
        } catch (SQLException throwables) {
            return null;
        }
        return connection;
    }

    public void run() {
        Statement state = null;
        Connection conn = null;
        try {
            this.sem.acquire();
            conn = getConnection();
            state = conn.createStatement();
            double start = System.currentTimeMillis();
            ResultSet resultSet = state.executeQuery(this.sql);
            while (resultSet.next());
            System.out.println(this.sql);
            double end = System.currentTimeMillis();
            LOG.info("execute sql " + this.sql + " cost time " + ((end - start) / 1000.0D));
        } catch (Exception e) {
            this.ex.set(e);
        } finally {
            this.sem.release();
            try {
                if (state != null)
                    conn.close();
            } catch (SQLException sQLException) {}
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException sQLException) {}
        }
    }
}

