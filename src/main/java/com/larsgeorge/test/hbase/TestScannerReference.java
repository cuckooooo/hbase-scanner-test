package com.larsgeorge.test.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

public class TestScannerReference {

  private static void printScanMetrics(Scan scan) throws IOException {
    byte[] data = scan.getAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA);
    if (data != null) {
      ByteArrayInputStream bais = new ByteArrayInputStream(data);
      ScanMetrics metrics = new ScanMetrics();
      metrics.readFields(new DataInputStream(bais));
      System.out.println("RPC calls: " + metrics.countOfRPCcalls.getCurrentIntervalValue() +
        " (" + metrics.countOfRPCcalls.getPreviousIntervalValue() + ")");
      System.out.println("Remote RPC calls: " +
        metrics.countOfRemoteRPCcalls.getCurrentIntervalValue() + " (" +
        metrics.countOfRemoteRPCcalls.getPreviousIntervalValue() + ")");
    } else {
      System.out.println("No scan metrics found.");
    }
  }

  public static void main(String[] args) throws IOException {
    Logger logger = Logger.getLogger("org.apache.hadoop.hbase");
    logger.setLevel(Level.DEBUG);

    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.client.log.scanner.activity", true);
    conf.setInt("hbase.client.log.scanner.latency.cutoff", -1000);
    conf.setInt("hbase.client.pause", 250); // fail faster
    conf.setInt("hbase.client.retries.number", 2); // try less often

    HBaseHelper helper = HBaseHelper.getHelper(conf); // increases connection refcount
    helper.dropTable("testtable");
    helper.createTable("testtable", "cf1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 50, 5, 2, false, "cf1");
    helper.close(); // decrease connection refcount

    System.out.println("=============== TEST 1 - Using HTable ==============");

    HTable table1 = new HTable(conf, "testtable"); // increases refcount
    HConnection conn1 = HConnectionManager.getConnection(conf); // increases refcount
    HConnectionManager.deleteConnection(conf); // decrease refcount, still a valid object reference

    Scan scan1 = new Scan();
    scan1.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(true));
    scan1.setCaching(1); // cause RPC per row
    ResultScanner scanner1 = table1.getScanner(scan1);
    try {
      System.out.println("Scanning table #1...");
      int count1 = 0;
      for (Result res : scanner1) {
        System.out.println(res);
        if (count1 == 9) {
          System.out.println("Closing table #1...");
          table1.close(); // decrease refcount, while scanner is still active
          System.out.println("Connection closed: " + conn1.isClosed());
          // the test fails badly hereafter since the connection is now gone
          // but the loop and scanner try to access ZooKeeper and fail doing so
        }
        count1++;
      }
      scanner1.close();
      System.out.println("Connection closed: " + conn1.isClosed());
    } catch (Exception e) {
      e.printStackTrace();
    }
    printScanMetrics(scan1);

    System.out.println("=============== TEST 2 - Using HTablePool ==============");

    System.out.println("Creating table pool...");
    HTablePool pool = new HTablePool(conf, 2);
    HTableInterface table2a = pool.getTable("testtable");
    HTableInterface table2b = pool.getTable("testtable");
    HTableInterface table2c = pool.getTable("testtable");

    HConnection conn2 = HConnectionManager.getConnection(conf); // increases refcount
    HConnectionManager.deleteConnection(conf); // decrease refcount, still a valid object reference

    Scan scan2 = new Scan();
    scan2.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(true));
    scan2.setCaching(1);
    ResultScanner scanner2 = table2c.getScanner(scan2);
    try {
      System.out.println("Scanning table #2...");
      int count2 = 0;
      for (Result res : scanner2) {
        System.out.println(res);
        if (count2 == 9) {
          System.out.println("Current pool size: " + pool.getCurrentPoolSize("testtable"));
          System.out.println("Closing table #2...");
          table2c.close();
          System.out.println("Connection closed: " + conn2.isClosed());
          System.out.println("Current pool size: " + pool.getCurrentPoolSize("testtable"));
        }
        if (count2 == 19) {
          System.out.println("Current pool size: " + pool.getCurrentPoolSize("testtable"));
          System.out.println("Returning all tables...");
          table2a.close();
          table2b.close();
          System.out.println("Connection closed: " + conn2.isClosed());
          System.out.println("Current pool size: " + pool.getCurrentPoolSize("testtable"));
        }
        if (count2 == 29) {
          System.out.println("Closing table pool...");
          pool.close();
          System.out.println("Connection closed: " + conn2.isClosed());
          System.out.println("Current pool size: " + pool.getCurrentPoolSize("testtable"));
        }
        count2++;
      }
      scanner2.close();
      System.out.println("Connection closed: " + conn2.isClosed());
    } catch (Exception e) {
      e.printStackTrace();
    }
    printScanMetrics(scan2);
  }
}
