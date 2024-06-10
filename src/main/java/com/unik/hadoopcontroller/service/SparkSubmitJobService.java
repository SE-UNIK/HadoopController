package com.unik.hadoopcontroller.service;

import com.unik.hadoopcontroller.CustomMultipartFile;
import com.unik.hadoopcontroller.model.SparkModel;
import org.apache.spark.launcher.SparkLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.avro.AvroParquetReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

@Service
public class SparkSubmitJobService {

    private final String systemHadoopRootDir = "/home/hadoop";
    private final String systemSparkAlgorithmsDir = systemHadoopRootDir + "/spark/algorithms/";
    private final String hdfsRootDir = "/user/hadoop/";

    @Autowired
    private FileSystem fileSystem;

    @Autowired
    private Configuration hadoopConfiguration;

    @Autowired
    private HdfsDirectService hdfsDirectService;

    @Autowired
    private SparkModel sparkModel;

    private void redirectOutput(InputStream inputStream, OutputStream outputStream) {
        Executors.newSingleThreadExecutor().submit(() -> {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    writer.write(line);
                    writer.newLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public File launchSparkJob(SparkModel sparkJobModel, String fileName) {
        Process spark = null;
        File tempOutputFile = null;
        try {
            String inputFilePath = hdfsRootDir + sparkJobModel.getInputDirectoryPath() + sparkJobModel.getInputFileName();

            spark = new SparkLauncher()
                    .setSparkHome(systemHadoopRootDir + "/spark")
                    .setAppResource(systemSparkAlgorithmsDir + sparkJobModel.getAlgorithmName())
                    .setMaster("yarn")
                    .setDeployMode("cluster")
                    .addAppArgs(inputFilePath)
                    .setVerbose(true)
                    .launch();

            String analysisFileName = "analysis_results_" + fileName;
            String title = fileName + "'s Analysis Results";
            List<String> authors = Arrays.asList("Sparky");
            String systemOutputFilePath = "output/" + analysisFileName;

            if (!Files.exists(Paths.get("output"))) {
                Files.createDirectories(Paths.get("output"));
            }

            tempOutputFile = new File(systemOutputFilePath);
            File stdoutFile = new File("output/stdout.log");
            File stderrFile = new File("output/stderr.log");

            try (FileOutputStream stdoutStream = new FileOutputStream(stdoutFile);
                 FileOutputStream stderrStream = new FileOutputStream(stderrFile)) {
                // Redirect both stdout and stderr to separate files
                redirectOutput(spark.getInputStream(), stdoutStream);
                redirectOutput(spark.getErrorStream(), stderrStream);
            }

            int exitCode = spark.waitFor();
            System.out.println("Spark job finished with exit code: " + exitCode);

            // Combine stdout and stderr into a single file
            try (FileOutputStream outputStream = new FileOutputStream(tempOutputFile, true);
                 FileInputStream stdoutInputStream = new FileInputStream(stdoutFile);
                 FileInputStream stderrInputStream = new FileInputStream(stderrFile)) {
                byte[] buffer = new byte[1024];
                int length;
                while ((length = stdoutInputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, length);
                }
                while ((length = stderrInputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, length);
                }
            }

            // Read the temporary output file and write it to HDFS
            MultipartFile results = new CustomMultipartFile(tempOutputFile);
            hdfsDirectService.writeToHdfsUniqueWithFilePath(sparkJobModel.getOutputDirectoryPath(), analysisFileName, results, title, authors);

            return tempOutputFile;
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            System.out.println("Spark job was not finished successfully.");
            return null;
        } finally {
            if (spark != null) {
                spark.destroy();
            }
            if (tempOutputFile != null && tempOutputFile.exists()) {
                tempOutputFile.delete();
            }
        }
    }
    public List<Map<String, Object>> getAnalysisResults() throws IOException {
        String resultsFilePathStr = "/user/hadoop/analysis/results.parquet";
        Path resultsFilePath = new Path(resultsFilePathStr);
        List<Map<String, Object>> results = new ArrayList<>();

        if (!fileSystem.exists(resultsFilePath)) {
            throw new FileNotFoundException("Analysis results file not found in HDFS: " + resultsFilePathStr);
        }

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(HadoopInputFile.fromPath(resultsFilePath, hadoopConfiguration)).build()) {
            GenericRecord record;
            Schema schema = getAnalysisResultsSchema();
            while ((record = reader.read()) != null) {
                Map<String, Object> resultMap = new HashMap<>();
                for (Schema.Field field : schema.getFields()) {
                    Object value = record.get(field.name());
                    if (value instanceof CharSequence) {
                        value = value.toString();
                    } else if (value instanceof GenericData.Array) {
                        List<String> stringList = new ArrayList<>();
                        for (Object item : (GenericData.Array<?>) value) {
                            stringList.add(item.toString());
                        }
                        value = stringList;
                    }
                    resultMap.put(field.name(), value);
                }
                results.add(resultMap);
            }
        }

        return results;
    }

    private Schema getAnalysisResultsSchema() {
        String schemaJson = "{"
                + "\"type\":\"record\","
                + "\"name\":\"AnalysisResults\","
                + "\"fields\":["
                + "  {\"name\":\"id\", \"type\":\"string\"},"
                + "  {\"name\":\"result\", \"type\":\"string\"}"
                + "]}";
        return new Schema.Parser().parse(schemaJson);
    }

}
