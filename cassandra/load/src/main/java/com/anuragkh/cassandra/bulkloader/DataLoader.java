package com.anuragkh.cassandra.bulkloader;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataLoader {

  /**
   * Default output directory
   */
  public static final String DEFAULT_OUTPUT_DIR = "./data";

  /**
   * Keyspace name
   */
  public static final String KEYSPACE = "bench";
  /**
   * Table name
   */
  public static final String TABLE = "data";
  /**
   * Columns
   */
  public static final int numColumns = 105;
  public static final String[] columns;

  static {
    columns = new String[numColumns];
    columns[0] = "id";
    for (int i = 1; i < numColumns; i++) {
      columns[i] = ("field" + (i - 1));
    }
  }

  /**
   * Schema for bulk loading table.
   * It is important not to forget adding keyspace name before table name,
   * otherwise CQLSSTableWriter throws exception.
   */
  public static final String SCHEMA;

  static {
    String schemaStr = "CREATE TABLE " + KEYSPACE + "." + TABLE + " (";
    schemaStr += "id bigint, ";
    for (int i = 1; i < numColumns; i++) {
      schemaStr += columns[i] + " text, ";
    }
    schemaStr += "PRIMARY KEY(id) ";
    schemaStr += ") WITH COMPACT STORAGE ";
    schemaStr += "and compaction = {\'class\' : \'SizeTieredCompactionStrategy\' } ";
    schemaStr += "and compression = { \'sstable_compression\' : \'\' };";
    SCHEMA = schemaStr;
  }

  /**
   * INSERT statement to bulk load.
   * It is like prepared statement. You fill in place holder for each data.
   */
  public static final String INSERT_STMT;

  static {
    String insertStmtStr = "INSERT INTO " + KEYSPACE + "." + TABLE + " (";
    insertStmtStr += "id, ";
    for (int i = 1; i < numColumns; i++) {
      insertStmtStr += columns[i];
      if (i != numColumns - 1) {
        insertStmtStr += ", ";
      }
    }
    insertStmtStr += ") VALUES ( ";
    for (int i = 0; i < numColumns; i++) {
      insertStmtStr += "?";
      if (i != numColumns - 1) {
        insertStmtStr += ", ";
      }
    }
    insertStmtStr += ")";
    INSERT_STMT = insertStmtStr;
  }

  public static final CsvPreference CSV_PREFERENCE =
    (new CsvPreference.Builder('\"', 124, "\r\n")).build();

  public static void main(String[] args) {
    if (args.length < 1 || args.length > 2) {
      System.out.println("usage: bulkload <filePath> [<seed>]");
      return;
    }

    String filePath = args[0];
    long seed = 0L;
    if (args.length == 2) {
      seed = Long.parseLong(args[1]);
    }

    // magic!
    Config.setClientMode(true);

    // Create output directory that has keyspace and table name in the path
    File outputDir =
      new File(DEFAULT_OUTPUT_DIR + File.separator + KEYSPACE + File.separator + TABLE);
    if (!outputDir.exists() && !outputDir.mkdirs()) {
      throw new RuntimeException("Cannot create output directory: " + outputDir);
    }

    // Prepare SSTable writer
    CQLSSTableWriter.Builder builder = CQLSSTableWriter.builder();
    builder.inDirectory(outputDir).forTable(SCHEMA).using(INSERT_STMT)
      .withPartitioner(new Murmur3Partitioner());
    CQLSSTableWriter writer = builder.build();


    try (
      BufferedReader reader = new BufferedReader(new FileReader(filePath));
      CsvListReader csvReader = new CsvListReader(reader, CSV_PREFERENCE)
    ) {

      csvReader.getHeader(false);

      // Write to SSTable while reading data
      List<String> line;
      Long id = seed;
      while ((line = csvReader.read()) != null) {
        ArrayList<Object> values = new ArrayList<>();
        values.add(id);
        values.addAll(line);
        if (values.size() != numColumns) {
          System.out.println("Error: Row Size=" + values.size() + " Expected size=" + numColumns);
          System.exit(-1);
        }
        writer.addRow(values);
        id++;
      }
    } catch (InvalidRequestException | IOException e) {
      e.printStackTrace();
    }

    try {
      writer.close();
    } catch (IOException ignore) {
      System.out.println("[WARNING] Could not close writer.");
    }
  }
}
