package com.brebu.jdbcresttemplate.controller;

import com.brebu.jdbcresttemplate.model.ColumnType;
import com.brebu.jdbcresttemplate.model.FolderResult;
import com.brebu.jdbcresttemplate.util.ColumnTypeMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.*;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@RestController
public class JDBCController {

    @Autowired
    JdbcTemplate jdbcTemplate;


    @GetMapping("/insert")
    public int batchInsert() {

        System.out.println("Start" + System.currentTimeMillis());

        Map<String, String> map = new HashMap<>();
        String tableName = "TESTARE";

        String sql = "SELECT column_name,data_type FROM user_tab_cols where table_name= ?";
        List<ColumnType> list = jdbcTemplate.query(sql, new ColumnTypeMapper(), tableName);
        list.forEach(columnType -> map.put(columnType.getName(), columnType.getType()));

        try (
                InputStream is = new FileInputStream("/home/cnb/pomi.csv");
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ) {
            createBaos(is, baos);
            CSVFormat csvFormat = CSVFormat.Builder.create().setSkipHeaderRecord(false).setDelimiter(",").build();
            try (InputStream is1 = new ByteArrayInputStream(baos.toByteArray());
                 InputStream is2 = new ByteArrayInputStream(baos.toByteArray());
                 CSVParser csvParserInsert = csvFormat.parse(new BufferedReader(new InputStreamReader(is1)));
                 CSVParser csvParserCount = csvFormat.parse(new BufferedReader(new InputStreamReader(is2)))) {

                Iterator<CSVRecord> iteratorInsert = csvParserInsert.iterator();
                Iterator<CSVRecord> iteratorCount = csvParserCount.iterator();
                CSVRecord csvHeader = iteratorInsert.next();
                StringBuilder values = getValues(csvHeader);
                String header = csvHeader.stream().collect(Collectors.joining(","));
                int finalCount = getFinalCount(iteratorCount);

                int fin = this.jdbcTemplate.batchUpdate(
                        "insert into testare (" + header + ") values(" + values + ")",
                        new BatchPreparedStatementSetter() {

                            @Override
                            public void setValues(PreparedStatement ps, int i) throws SQLException {
                                CSVRecord csvRecord = iteratorInsert.next();
                                for (int j = 0; j < csvHeader.size(); j++) {
                                    String type = map.get(csvHeader.get(j));
                                    switch (type) {
                                        case "VARCHAR2": {
                                            ps.setString(j + 1, csvRecord.get(j));
                                            break;
                                        }
                                        case "NUMBER": {
                                            ps.setBigDecimal(j + 1, new BigDecimal(csvRecord.get(j)));
                                            break;
                                        }
                                    }
                                }
                            }

                            @Override
                            public int getBatchSize() {
                                return finalCount - 1;
                            }
                        }).length;
                System.out.println("Stop" + System.currentTimeMillis());
                return fin;
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return 0;
    }


    @GetMapping("/insertBatch")
    public int batchInsertBatch() throws IOException {

        System.out.println("Start" + System.currentTimeMillis());

        Map<String, String> map = new HashMap<>();
        String tableName = "TESTARE";

        String sql = "SELECT column_name,data_type FROM user_tab_cols where table_name= ?";
        List<ColumnType> list = jdbcTemplate.query(sql, new ColumnTypeMapper(), tableName);
        list.forEach(columnType -> map.put(columnType.getName(), columnType.getType()));
        CSVFormat csvFormat = CSVFormat.Builder.create().setSkipHeaderRecord(false).setDelimiter(",").build();

        FolderResult folder = getFolderResult();
        System.out.println(folder.getFolder());

        List<Integer> fileList = folder.getIntegerList();

        final CSVRecord[] csvHeader = {null};
        AtomicInteger cnt = new AtomicInteger();

        fileList.forEach(integer -> {
            System.out.println(integer);
            try (
                    InputStream is = new FileInputStream(folder.getFolder() + "/" + integer);
                    CSVParser csvParser = csvFormat.parse(new BufferedReader(new InputStreamReader(is)))
            ) {

                List<CSVRecord> csvRecords = csvParser.getRecords();
                if (csvHeader[0] == null) {
                    csvHeader[0] = csvRecords.get(0);
                    csvRecords.remove(0);
                }

                StringBuilder values = getValues(csvHeader[0]);
                String header = csvHeader[0].stream().collect(Collectors.joining(","));

                CSVRecord finalCsvHeader = csvHeader[0];
                int[][] updateCounts = jdbcTemplate.batchUpdate(
                        "insert into testare (" + header + ") values(" + values + ")",
                        csvRecords,
                        10000,
                        (ps, argument) -> {
                            for (int j = 0; j < finalCsvHeader.size(); j++) {
                                String type = map.get(finalCsvHeader.get(j));
                                switch (type) {
                                    case "VARCHAR2": {
                                        ps.setString(j + 1, argument.get(j));
                                        break;
                                    }
                                    case "NUMBER": {
                                        ps.setBigDecimal(j + 1, new BigDecimal(argument.get(j)));
                                        break;
                                    }
                                }
                            }
                        });
                cnt.addAndGet(updateCounts.length * 10000);
                System.gc();
                System.out.println(integer + " " + System.currentTimeMillis());

            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        });

        FileUtils.deleteDirectory(folder.getFolder());
        return cnt.get();
    }

    private int getFinalCount(Iterator<CSVRecord> iteratorCount) {
        int count = 0;
        while (iteratorCount.hasNext()) {
            iteratorCount.next();
            count++;
        }
        return count;
    }

    private StringBuilder getValues(CSVRecord csvHeader) {
        StringBuilder values = new StringBuilder();

        for (int i = 0; i < csvHeader.size(); i++) {
            values.append("?");
            if (i < csvHeader.size() - 1) {
                values.append(",");
            }
        }
        return values;
    }

    private void createBaos(InputStream is, ByteArrayOutputStream baos) throws IOException {
        byte[] buffer = new byte[1024];
        int len;
        while ((len = is.read(buffer)) > -1) {
            baos.write(buffer, 0, len);
        }
        baos.flush();
    }

    private FolderResult getFolderResult() throws IOException {
        long generatedLong = new Random().nextLong();
        File folder = new File("/home/cnb/" + generatedLong);
        List<Integer> integers = new ArrayList<>();
        if (!folder.exists()) {
            boolean result = folder.mkdirs();
            if (result) {
                System.out.println("The folder " + folder + " was created!");
            }
        }

        File file = new File("/home/cnb/pomi.csv");


        long linesWritten = 0;
        int count = 1;

        try (InputStream initialStream = FileUtils.openInputStream(file);
             BufferedReader reader = new BufferedReader(new InputStreamReader(initialStream))) {

            String line = reader.readLine();

            while (line != null) {
                File outFile = new File(folder + "/" + count);
                Writer writer = new OutputStreamWriter(new FileOutputStream(outFile));

                while (line != null && linesWritten < 1000000) {
                    writer.write(line);
                    writer.append("\n");
                    line = reader.readLine();
                    linesWritten++;
                }

                writer.close();
                linesWritten = 0;
                integers.add(count);
                count++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        integers.sort(Integer::compareTo);
        return new FolderResult(folder, integers);
    }
}
