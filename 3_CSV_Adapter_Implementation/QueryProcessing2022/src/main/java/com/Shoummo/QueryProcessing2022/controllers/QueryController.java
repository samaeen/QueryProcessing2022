package com.Shoummo.QueryProcessing2022.controllers;

import com.Shoummo.QueryProcessing2022.services.CsvService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import com.Shoummo.QueryProcessing2022.services.CsvService;

import java.sql.*;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@CrossOrigin(origins = "*", allowedHeaders = "*")
public class QueryController {
    private final CsvService csvService;

    public QueryController(CsvService csvService) {
        this.csvService = csvService;
    }

    @PostMapping("/query")
    public List<Map<String, Object>> query(HttpEntity<String> httpEntity) throws SQLException {
        String query = httpEntity.getBody();
//        System.out.println(query);
        log.debug("Running '{}'", query);

        List<Map<String, Object>> results = csvService.query(query);
        log.debug("Retrieved {} rows", results.size());
//        System.out.println(results.toString());
        return results;
    }

}
