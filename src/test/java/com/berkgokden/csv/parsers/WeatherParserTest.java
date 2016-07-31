package com.berkgokden.csv.parsers;

import com.berkgokden.csv.IndexableContent;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by developer on 7/29/16.
 */
public class WeatherParserTest {
    private static final long ONE_HOUR_IN_SECONDS = 3600L;
    @Test
    public void shoudlPassWhenDateTimeConvertedCorrectly() throws Exception {
        /*
        String[] values = new String[] {"MSWRMOS",
                "mix  2015090218",
                "62830",
                "2015-09-03",
                "02:00",
                "00100",
                "     95",
                "95",
                "148",
                "228",
                "2",
                "3"};
        Map<String, Integer> mapping  = new HashMap<>(WeatherParser.Headers.values().length);
        for (WeatherParser.Headers headers : WeatherParser.Headers.values()) {
            mapping.put(headers.name(), headers.ordinal());
        }
        */

        // to test private methods reflection is needed
        Method method = WeatherParser.class
                .getDeclaredMethod("convertDateToUnixtime",
                        String.class, String.class, String.class);
        method.setAccessible(true);
        Long time = (Long) method.invoke(WeatherParser.class, "2015-09-03", "02:00", "00100");
        System.out.println(time+"s");

        Long expectedUnixTime = 1441249200L;
        assertEquals(expectedUnixTime,
                method.invoke(WeatherParser.class, "2015-09-03", "02:00", "00100"));
        expectedUnixTime += ONE_HOUR_IN_SECONDS;
        assertEquals(expectedUnixTime,
                method.invoke(WeatherParser.class, "2015-09-03", "02:00", "00200"));

    }

    @Test
    public void shoudlPassWhenParsed() throws Exception {
        String[] values = new String[] {"MSWRMOS",
                "mix  2015090218",
                "62830",
                "2015-09-03",
                "02:00",
                "00100",
                "     95",
                "95",
                "148",
                "228",
                "2",
                "3"};
        Map<String, Integer> mapping  = new HashMap<>(WeatherParser.Headers.values().length);
        for (WeatherParser.Headers headers : WeatherParser.Headers.values()) {
            mapping.put(headers.name(), headers.ordinal());
        }

        /*
        for (Constructor<?> constructor : CSVRecord.class.) {
            System.out.println("=> "+constructor.getParameterTypes());
        }*/


        Constructor<CSVRecord> constructor = CSVRecord.class.getDeclaredConstructor(
                        String[].class, Map.class, String.class, long.class, long.class);
        constructor.setAccessible(true);

        CSVRecord record = constructor
                .newInstance(values, mapping, null, 0L, 0L);

        IndexableContent indexableContent = WeatherParser.parse(record);

        System.out.println(indexableContent.getId());

    }
}