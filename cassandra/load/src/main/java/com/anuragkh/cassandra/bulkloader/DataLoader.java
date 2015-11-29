package com.anuragkh.cassandra.bulkloader;

import org.apache.cassandra.config.Config;
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
    columns[0] = "_id";
    for (int i = 1; i < numColumns; i++) {
      columns[i] = ("field" + (i - 1) + ", ");
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
    schemaStr += "_id bigint, ";
    for (int i = 1; i < numColumns; i++) {
      schemaStr += columns[i] + " string, ";
    }
    schemaStr += "PRIMARY KEY(_id) ";
    schemaStr += ") WITH COMPACT STORAGE ";
    schemaStr += "and compaction = {\'class\' : \'SizeTieredCompactionStrategy\' } ";
    schemaStr += "and compression = { \'sstable_compression\' : \'\' };";
    SCHEMA = schemaStr;
    System.out.println(SCHEMA);
  }

  //  public static final String SCHEMA = String.format("CREATE TABLE %s.%s (" +
  //    "ticker ascii, " +
  //    "date timestamp, " +
  //    "open decimal, " +
  //    "high decimal, " +
  //    "low decimal, " +
  //    "close decimal, " +
  //    "volume bigint, " +
  //    "adj_close decimal, " +
  //    "PRIMARY KEY (ticker, date) " +
  //    ") WITH CLUSTERING ORDER BY (date DESC)", KEYSPACE, TABLE);


  /**
   * INSERT statement to bulk load.
   * It is like prepared statement. You fill in place holder for each data.
   */
  public static final String INSERT_STMT;

  static {
    String insertStmtStr = "INSERT INTO " + KEYSPACE + "." + TABLE + " (";
    insertStmtStr += "_id, ";
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
    System.out.println(INSERT_STMT);
  }

  //  public static final String INSERT_STMT = String.format("INSERT INTO %s.%s (" +
  //    "ticker, date, open, high, low, close, volume, adj_close" +
  //    ") VALUES (" +
  //    "?, ?, ?, ?, ?, ?, ?, ?" +
  //    ")", KEYSPACE, TABLE);

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
    builder.inDirectory(outputDir).forTable(SCHEMA).using(INSERT_STMT);
    CQLSSTableWriter writer = builder.build();

    Long id = seed;
    try (
      BufferedReader reader = new BufferedReader(new FileReader(filePath));
      CsvListReader csvReader = new CsvListReader(reader, CsvPreference.STANDARD_PREFERENCE)
    ) {

      csvReader.getHeader(false);

      // Write to SSTable while reading data
      List<String> line;
      while ((line = csvReader.read()) != null) {
        // We use Java types here based on
        // http://www.datastax.com/drivers/java/2.0/com/datastax/driver/core/DataType.Name.html#asJavaClass%28%29
        ArrayList<Object> values = new ArrayList<>();
        values.add(id);
        values.addAll(line);
        writer.addRow(values);
//        writer.addRow(ticker, DATE_FORMAT.parse(line.get(0)), new BigDecimal(line.get(1)),
//          new BigDecimal(line.get(2)), new BigDecimal(line.get(3)), new BigDecimal(line.get(4)),
//          Long.parseLong(line.get(5)), new BigDecimal(line.get(6)));
      }
    } catch (InvalidRequestException | IOException e) {
      e.printStackTrace();
    }


    try {
      writer.close();
    } catch (IOException ignore) {
    }
  }
}
