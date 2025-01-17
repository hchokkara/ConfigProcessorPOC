package org.bss;

import java.sql.*;
import java.util.*;

public class SqlHelper {
    public String generateUpdateSqlStatement(String tableName, String updatingColumnName, String updatedValue, String filterColumnName, String filterColumnValue) {
        // Generate SQL statement
        String sql = String.format(
                "UPDATE %s SET %s = '%s' WHERE %s = '%s';",
                tableName, updatingColumnName, updatedValue.replace("'", "''"), filterColumnName, filterColumnValue
        );
        return sql;
    }

    public Map<String, String> getConfigDataFromDB(String databaseUrl, String databaseUserName, String databasePassword, List<String> docTypeIds) {
//        databaseUrl = "jdbc:mysql://bluesage-lower-cluster.cluster-c4bji9jquct6.us-east-1.rds.amazonaws.com/bluesage_dev?useSSL=false&rewriteBatchedStatements=true&cachePrepStmts=true";
//        databaseUserName = "bss_developers";
//        databasePassword = "CvR96pjhH8mrLVPZ";
//        docTypeIds = new ArrayList<>();
//        docTypeIds.add("PXL031");
//        docTypeIds.add("PXL021");

        String sqlCommand = String.format(
                "SELECT document_type_id, config_json FROM ref_document_type WHERE document_type_id in (%s)",
                String.join(",", Collections.nCopies(docTypeIds.size(), "?"))
        );

        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        Map<String, String> configJsonMap = new HashMap<>();
        try {
            connection = DriverManager.getConnection(databaseUrl, databaseUserName, databasePassword);
            preparedStatement = connection.prepareStatement(sqlCommand);
            // Set the parameters
            for (int i = 0; i < docTypeIds.size(); i++) {
                preparedStatement.setString(i + 1, docTypeIds.get(i));
            }

            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                String docTypeId = resultSet.getString(1);
                String config = resultSet.getString(2);
                configJsonMap.put(docTypeId, config);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // Close the database connection and result set objects
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return configJsonMap;
    }
}
