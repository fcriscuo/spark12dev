package org.mskcc.cbio.spark12dev.genomics;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


import java.io.FileReader;
import java.io.Reader;
import java.util.List;

/**
 * Created by fcriscuo on 3/15/15.
 */
public class CosmicSparkSQL {
    /*
    Represents a class that can support in-memory SQL queries against the complete Cosmic Mutation File
     */

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sqlCtx = new SQLContext(ctx);
        // read in all the Cosmic data into an RDD
        Reader reader = new FileReader("/data/cosmic/cosmic_mutation_demo.tsv");
        CSVParser parser = new CSVParser(reader, CSVFormat.TDF.withHeader().withIgnoreEmptyLines(true));
        List<CSVRecord> recordList = parser.getRecords();
        JavaRDD<CosmicRecord> cosmicRDD = ctx.parallelize(recordList).map(new Function<CSVRecord, CosmicRecord>() {
            @Override
            public CosmicRecord call(CSVRecord csvRecord) throws Exception {
                return new CosmicRecord(csvRecord);
            }
        });
        DataFrame cosmicSchema = sqlCtx.createDataFrame(cosmicRDD, CosmicRecord.class);
        cosmicSchema.registerTempTable("cosmic");
        // create a data frame of PTEN mutations
        String ptenQuery = "SELECT geneName, geneCdsLength, idTumor from cosmic where geneName = 'PTEN'";
        DataFrame ptenRDD = sqlCtx.sql(ptenQuery);
        List<String> ptenResults = ptenRDD.toJavaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) {

                return "Gene Name: " + row.getString(0) + " CDS Length " + row.getString(1) +
                        " Tumor ID " + row.getString(2);

            }
        }).collect();
        for (String line : ptenResults) {
            System.out.println(line);
        }

        System.out.println("=== Data source: Parquet File ===");
        // DataFrames can be saved as parquet files, maintaining the schema information.
        cosmicSchema.saveAsParquetFile("/tmp/spark/resources/cosmic_sample.parquet");
        

    }

    public static class CosmicRecord {
        private String geneName;
        private String accessionNumber;
        private String geneCdsLength;
        private String hgncId;
        private String sampleName;
        private String idSample;
        private String idTumor;
        private String primarySite;
        private String siteSubtype;
        private String primaryHistology;
        private String histologySubtype;
        private String genomeWideScreen;
        private String mutationId;
        private String mutationCDS;
        private String mutationAA;
        private String mutationDescription;
        private String mutationZygosity;
        private String mutationGRCh37GenomePosition;
        private String mutationGRCh37GenomeStrand;
        private String snp;
        private String fathmmPrediction;
        private String mutationSomaticStatus;
        private String pubmedPMID;
        private String idStudy;
        private String sampleSource;
        private String tumorOrigin;
        private String age;
        private String comments;


        public CosmicRecord(CSVRecord aRecord) {
            this.geneName = aRecord.get("Gene name");
            this.accessionNumber = aRecord.get("Accession Number");
            this.geneCdsLength = aRecord.get("Gene CDS length");
            this.hgncId = aRecord.get("HGNC ID");
            this.sampleName = aRecord.get("Sample name");
            this.idSample = aRecord.get("ID_sample");
            this.idTumor = aRecord.get("ID_tumour"); // n.b. British spelling
            this.primarySite = aRecord.get("Primary site");
            this.siteSubtype = aRecord.get("Site subtype");
            this.primaryHistology = aRecord.get("Primary histology");
            this.histologySubtype = aRecord.get("Histology subtype");
            this.genomeWideScreen = aRecord.get("Genome-wide screen");
            this.mutationId = aRecord.get("Mutation ID");
            this.mutationCDS = aRecord.get("Mutation CDS");
            this.mutationAA = aRecord.get("Mutation AA");
            this.mutationDescription = aRecord.get("Mutation Description");
            this.mutationZygosity = aRecord.get("Mutation zygosity");
            this.mutationGRCh37GenomePosition = aRecord.get("Mutation GRCh37 genome position");
            this.mutationGRCh37GenomeStrand = aRecord.get("Mutation GRCh37 strand");
            this.snp = aRecord.get("SNP");
            this.fathmmPrediction = aRecord.get("FATHMM prediction");
            this.mutationSomaticStatus = aRecord.get("Mutation somatic status");
            this.pubmedPMID = aRecord.get("Pubmed_PMID");
            this.idStudy = aRecord.get("ID_STUDY");
            this.sampleSource = aRecord.get("Sample source");
            this.tumorOrigin = aRecord.get("Tumour origin");
            this.age = aRecord.get("Age");
            this.comments = aRecord.get("Comments");
        }

        public String getGeneName() {
            return geneName;
        }

        public String getAccessionNumber() {
            return accessionNumber;
        }

        public String getGeneCdsLength() {
            return geneCdsLength;
        }

        public String getHgncId() {
            return hgncId;
        }

        public String getSampleName() {
            return sampleName;
        }

        public String getIdSample() {
            return idSample;
        }

        public String getIdTumor() {
            return idTumor;
        }

        public String getPrimarySite() {
            return primarySite;
        }

        public String getSiteSubtype() {
            return siteSubtype;
        }

        public String getPrimaryHistology() {
            return primaryHistology;
        }

        public String getHistologySubtype() {
            return histologySubtype;
        }

        public String getGenomeWideScreen() {
            return genomeWideScreen;
        }

        public String getMutationId() {
            return mutationId;
        }

        public String getMutationCDS() {
            return mutationCDS;
        }

        public String getMutationAA() {
            return mutationAA;
        }

        public String getMutationDescription() {
            return mutationDescription;
        }

        public String getMutationZygosity() {
            return mutationZygosity;
        }

        public String getMutationGRCh37GenomePosition() {
            return mutationGRCh37GenomePosition;
        }

        public String getMutationGRCh37GenomeStrand() {
            return mutationGRCh37GenomeStrand;
        }

        public String getSnp() {
            return snp;
        }

        public String getFathmmPrediction() {
            return fathmmPrediction;
        }

        public String getMutationSomaticStatus() {
            return mutationSomaticStatus;
        }

        public String getPubmedPMID() {
            return pubmedPMID;
        }

        public String getIdStudy() {
            return idStudy;
        }

        public String getSampleSource() {
            return sampleSource;
        }

        public String getTumorOrigin() {
            return tumorOrigin;
        }

        public String getAge() {
            return age;
        }

        public String getComments() {
            return comments;
        }
    }
}

