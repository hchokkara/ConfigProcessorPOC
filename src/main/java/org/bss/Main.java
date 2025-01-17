package org.bss;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) throws JsonProcessingException {
        List<String[]> rows = processInputFile("data/input.csv");

        ConfigProcessorHelper configProcessorHelper = new ConfigProcessorHelper();
        SqlHelper sqlHelper = new SqlHelper();
        ObjectMapper objectMapper = new ObjectMapper();

        String sourceDatabaseUrl = "jdbc:mysql://bluesage-lower-cluster.cluster-c4bji9jquct6.us-east-1.rds.amazonaws.com/bluesage_dev?useSSL=false&rewriteBatchedStatements=true&cachePrepStmts=true";
        String sourceDatabaseUserName = "bss_developers";
        String sourceDatabasePassword = "CvR96pjhH8mrLVPZ";
        Map<String, String> sourceConfigs = sqlHelper.getConfigDataFromDB(sourceDatabaseUrl, sourceDatabaseUserName, sourceDatabasePassword, rows.stream().map(x -> x[0]).collect(Collectors.toList()));

        String destinationDatabaseUrl = "jdbc:mysql://dev-seq.cluster-cm711hj0bd1j.us-east-1.rds.amazonaws.com/springeq_dev?useSSL=false&rewriteBatchedStatements=true&cachePrepStmts=true";
        String destinationDatabaseUserName = "bss_developers";
        String destinationDatabasePassword = "CvR96pjhH8mrLVPZ";
        Map<String, String> destinationConfigs = sqlHelper.getConfigDataFromDB(destinationDatabaseUrl, destinationDatabaseUserName, destinationDatabasePassword, rows.stream().map(x -> x[1]).collect(Collectors.toList()));

        StringBuilder updateSqlBuilder = new StringBuilder();
        StringBuilder rollbackSqlBuilder = new StringBuilder();
        for (String[] row : rows) {
            String sourceDocTypeId = row[0];
            String destinationDocTypeId = row[1];
            String sourceConfigString = sourceConfigs.remove(sourceDocTypeId);
            String destinationConfigString = destinationConfigs.remove(destinationDocTypeId);

            // Generate rollback SQL based on action
            String rollbackSql = sqlHelper.generateUpdateSqlStatement("ref_document_type", "config_json", destinationConfigString.replace("'", "''"), "document_type_id", destinationDocTypeId);
            rollbackSqlBuilder.append(rollbackSql).append("\n").append("\n");

            ObjectNode sourceConfig = (ObjectNode) objectMapper.readTree(sourceConfigString);
            ObjectNode destinationConfig = (ObjectNode) objectMapper.readTree(destinationConfigString);
            JsonNode configDetails = objectMapper.readTree(row[2]);

            // Process configurations
            configProcessorHelper.processConfigurations(sourceConfig, destinationConfig, configDetails);

            // Generate update SQL based on action
            String updateSql = sqlHelper.generateUpdateSqlStatement("ref_document_type", "config_json", objectMapper.writeValueAsString(destinationConfig).replace("'", "''"), "document_type_id", destinationDocTypeId);
            updateSqlBuilder.append(updateSql).append("\n").append("\n");
        }


        // Write rollback SQL to a file
        String rollbackSqlFile = "data/rollbackSqlFile.sql";
        try (FileWriter writer = new FileWriter(rollbackSqlFile)) {
            writer.write(rollbackSqlBuilder.toString());
            System.out.println("SQL script saved to: " + rollbackSqlFile);
        } catch (IOException e) {
            System.err.println("Error writing SQL file: " + e.getMessage());
        }

        // Write update SQL to a file
        String updateSqlFile = "data/updateSqlFile.sql";
        try (FileWriter writer = new FileWriter(updateSqlFile)) {
            writer.write(updateSqlBuilder.toString());
            System.out.println("SQL script saved to: " + updateSqlFile);
        } catch (IOException e) {
            System.err.println("Error writing SQL file: " + e.getMessage());
        }
    }

    private static List processInputFile(String inputCsvPath) {
        List<String[]> rows = new ArrayList<>();
        try (Scanner scanner = new Scanner(new File(inputCsvPath))) {
            // Read the header row
            if (scanner.hasNextLine()) {
                scanner.nextLine(); // Skip the header
            }

            // Process each row in the CSV
            while (scanner.hasNextLine()) {
                String[] row = scanner.nextLine().split(",", 3);
                rows.add(row);
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        return rows;
    }
}