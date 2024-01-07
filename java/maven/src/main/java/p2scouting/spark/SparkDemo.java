package p2scouting.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

public class SparkDemo {
  public static void main(String[] args) {
    String inFile = args[0];
    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
    Dataset<Row> fileData = spark.read().format("binaryFile").load(inFile).cache();
    fileData.printSchema();
    fileData.show();
    RawFileSplitter splitter = new RawFileSplitter();
    Dataset<Row> rowData = fileData.flatMap(splitter, splitter.encoder());
    rowData.printSchema();
    rowData.show();
    EventUnpacker unpacker = new EventUnpacker();
    Dataset<Row> unpackedData = rowData.map(unpacker, unpacker.encoder()).cache();
    unpackedData.printSchema();
    unpackedData.show();    
    // Count events, orbits, puppi candidates, compute sum pt
    long norbits = unpackedData.select("orbit").count(); 
    long nevents = unpackedData.select("orbit").distinct().count(); 
    Dataset<Row> pts = unpackedData.select("puppis.pt");
    Dataset<Row> flatPts = pts.select(functions.explode(pts.col("pt")).as("pts"));
    Row summary = flatPts.agg(functions.count("pts").as("items"), functions.sum("pts").as("sumpt")).head();
    long puppis = summary.getAs("items");
    double sumpt = summary.getAs("sumpt");
    Dataset<Row> selectedData = unpackedData.filter(new SimpleSelection(15,10,7)).cache();
    long selectedEvents = selectedData.count();
    selectedData.write().json("selected.json");
    selectedData.write().parquet("selected.parquet");
    System.out.printf("Processed %d orbits, %d events, %d candidates, sumpt %.1f. Selected %d events.\n", norbits, nevents, puppis, sumpt, selectedEvents);

    spark.stop();
  }
}