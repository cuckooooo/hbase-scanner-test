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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import java.io.IOException;
import java.io.OutputStreamWriter;

public class TestScannerReference {

  public static void main(String[] args) throws IOException {
    Logger logger = Logger.getLogger("org.apache.hadoop.hbase");
    logger.setLevel(Level.DEBUG);

    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean("hbase.client.log.scanner.activity", true);
    conf.setInt("hbase.client.log.scanner.latency.cutoff", -1000);

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "cf1");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 30, 5, 2, false, "cf1");

    HConnection conn = HConnectionManager.getConnection(conf);

    HTable table1 = new HTable(conf, "testtable");
    System.out.println("Scanning table #1...");
    Scan scan1 = new Scan();
    scan1.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(true));
    scan1.setCaching(1);
    ResultScanner scanner1 = table1.getScanner(scan1);
    int count1 = 0;
    for (Result res : scanner1) {
      System.out.println(res);
      if (count1 == 10) {
        System.out.println("Closing table #1...");
        table1.close();
        HConnectionManager.deleteAllConnections();
        System.out.println("Connection closed: " + conn.isClosed());
      }
      count1++;
    }
    System.out.println("Connection closed: " + conn.isClosed());
    scanner1.close();
    System.out.println("Connection closed: " + conn.isClosed());
    System.out.println("Scan metrics: " + Bytes.toString(scan1.getAttribute(
      Scan.SCAN_ATTRIBUTES_METRICS_DATA)));

    System.out.println("Creating table pool...");
    HTablePool pool = new HTablePool(conf, 1);
    HTableInterface table2 = pool.getTable("testtable");

    System.out.println("Scanning table #2...");
    Scan scan2 = new Scan();
    scan2.setCaching(1);
    ResultScanner scanner2 = table1.getScanner(scan1);
    int count2 = 0;
    for (Result res : scanner1) {
      System.out.println(res);
      if (count2 == 10) {
        System.out.println("Closing table #2...");
        table2.close();
      }
      count2++;
    }
    scanner2.close();
  }

}
