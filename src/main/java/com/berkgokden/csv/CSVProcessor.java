package com.berkgokden.csv;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.*;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * A Helper class to container processCSVdata method
 */
public class CSVProcessor {

    /**
     * This metod parses a given data as csv with given variables
     * @param rawCSV csv file content as byte[]
     * @param delimiter csv file delimiter ex.: ";"
     * @param headerEnum Headers class of CSV file.
     * @param parserFunction function to add special logic
     * @param consumer a consumer function can be added to use processed data
     * @throws IOException io exception can help container to retry.
     */
    public static void processCSVdata(byte[] rawCSV,
                                      char delimiter,
                                      Class<? extends Enum<?>> headerEnum,
                                      Function<CSVRecord, IndexableContent> parserFunction,
                                      Consumer<IndexableContent> consumer) throws IOException {
        InputStream fin = new ByteArrayInputStream(rawCSV);
        Reader in = new BufferedReader(new InputStreamReader(fin));
        Iterable<CSVRecord> records = CSVFormat.newFormat(delimiter).withHeader(headerEnum).parse(in);
        for (CSVRecord record : records) {
            IndexableContent indexableContent = parserFunction.apply(record);
            if (indexableContent != null) { // skip line when null ex.: header
                consumer.accept(indexableContent);
            }
        }
    }
}
