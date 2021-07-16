package com.example.concurrency_test.test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcTemplate {
    private static Logger LOG = LoggerFactory.getLogger(JdbcTemplate.class);

    public static void readSqlToQueue(String path, BlockingQueue<String> queue) throws Exception {
        try(FileInputStream inputStream = new FileInputStream(path);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
            String str = null;
            while ((str = bufferedReader.readLine()) != null)
                queue.put(str);
        }
    }

    public static void main(String[] args) throws Exception {
        boolean b;
        BasicParser basicParser = new BasicParser();
        Options options = new Options();
        options.addOption("thread", true, "thread");
        options.addOption("filePath", true, "filePath");
        CommandLine line = basicParser.parse(options, args);
        Properties prop = new Properties();
        prop.load(new FileReader("config.properties"));
        int cycle = Integer.parseInt(prop.getProperty("cycle"));
        int thread = Integer.parseInt(prop.getProperty("thread"));
        String filePath = prop.getProperty("filePath");
        String jdbcUrl = prop.getProperty("jdbcUrl");
        String user = prop.getProperty("user");
        String password = prop.getProperty("password");
        if (line.hasOption("thread"))
            thread = Integer.parseInt(line.getOptionValue("thread"));
        if (line.hasOption("filePath"))
            filePath = line.getOptionValue("filePath");
        Semaphore semaphore = new Semaphore(thread);
        BlockingQueue<String> queue = new ArrayBlockingQueue<>(500);
        for (int i = 0; i < cycle; i++)
            readSqlToQueue(filePath, queue);
        Executor executor = Executors.newCachedThreadPool();
        AtomicReference<Exception> ex = new AtomicReference<>();
        double start = System.currentTimeMillis();
        while (true) {
            String sql = queue.poll(2L, TimeUnit.SECONDS);
            if (sql != null) {
                executor.execute((Runnable)new RunSql(ex, semaphore, sql.trim(), jdbcUrl, user, password));
                if (ex.get() != null)
                    throw (Exception)ex.get();
                continue;
            }
            break;
        }
        do {
            b = semaphore.tryAcquire(thread, 5L, TimeUnit.SECONDS);
            if (ex.get() != null)
                throw (Exception)ex.get();
        } while (b != true);
        double end = System.currentTimeMillis();
        LOG.info("all cost time is " + ((end - start) / 1000.0D));
        System.exit(0);
    }
}
