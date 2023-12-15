package p2scouting.spark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;

public class SparkDemo {
  public static void main(String[] args) {
    String inFile = "/afs/cern.ch/work/g/gpetrucc/vivado/correlator-layer2/deregionizer_scouting/clients/native64.data";
    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
    Dataset<Row> fileData = spark.read().format("binaryFile").load(inFile);
    fileData.printSchema();
    fileData.show();
    RawFileSplitter splitter = new RawFileSplitter();
    Dataset<Row> rowData = fileData.flatMap(splitter, splitter.encoder());
    rowData.printSchema();
    rowData.show();
    EventUnpacker unpacker = new EventUnpacker();
    Dataset<Row> unpackedData = rowData.map(unpacker, unpacker.encoder());
    unpackedData.printSchema();
    unpackedData.show();    
    // Count events, orbits, puppi candidates, compue sum pt
    long norbits = unpackedData.select("orbit").count(); 
    long nevents = unpackedData.select("orbit").distinct().count(); 
    Dataset<Row> pts = unpackedData.select("puppis.pt");
    Dataset<Row> flatPts = pts.select(functions.explode(pts.col("pt")).as("pts"));
    Row summary = flatPts.agg(functions.count("pts").as("items"), functions.sum("pts").as("sumpt")).head();
    long puppis = summary.getAs("items");
    double sumpt = summary.getAs("sumpt");
    System.out.printf("Processed %d orbits, %d events, %d candidates, sumpt %.1f\n", norbits, nevents, puppis, sumpt);
    spark.stop();
  }
}