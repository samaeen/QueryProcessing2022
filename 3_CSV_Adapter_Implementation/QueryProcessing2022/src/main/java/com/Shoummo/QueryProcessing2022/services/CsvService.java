package com.Shoummo.QueryProcessing2022.services;

import com.Shoummo.QueryProcessing2022.configurations.ModelConfiguration;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.tomcat.util.descriptor.InputSourceUtil;
import org.springframework.stereotype.Service;
import com.Shoummo.QueryProcessing2022.configurations.ModelConfiguration;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class CsvService {
    private final ModelConfiguration modelConfiguration;

    public CsvService(ModelConfiguration modelConfiguration) {
        this.modelConfiguration = modelConfiguration;
    }

    public List<Map<String, Object>> query(String query) throws SQLException {
        Connection connection = DriverManager.getConnection("jdbc:calcite:model=inline:" + modelConfiguration.getModel());
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query);
        ResultSetMetaData metaData = resultSet.getMetaData();

        //RelOptCluster cluster = newCluster(typeFactory);

        List<Map<String, Object>> rows = new ArrayList<>();

        while (resultSet.next()) {
            Map<String, Object> row = new HashMap<>();
            for(var column = 1; column <= metaData.getColumnCount(); column++) {
                row.put(metaData.getColumnName(column), resultSet.getObject(column));
            }
            rows.add(row);
        }

        resultSet.close();
        statement.close();
        connection.close();

        return rows;
    }

}
