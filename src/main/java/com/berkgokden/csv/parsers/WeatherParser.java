package com.berkgokden.csv.parsers;

import com.berkgokden.csv.IndexableContent;
import org.apache.commons.csv.CSVRecord;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * WeatherParser can be used in conjunction with CSVProcessor Class
 * WeatherParser hold methods and variables to help parse and create IndexableContent
 * CSV text file containing weather data.
 * logic to skip header and unnecessary lines is integrated into parse method
 */
public class WeatherParser {
    protected static final String index = "weather";
    protected static final String type = "forecast";
    protected static final String OBSERVATION = "OBS";
    protected static final String FIRST_HEADER_VALUE = "PROJECT";
    protected static final String FORECASTDATETIME = "forecastdatetime";

    public static final char delimeter = ';';
    /*
    Headers are exact titles in the CSV file,
    the order of titles is important
     */
    public enum Headers {
        PROJECT,
        MODEL, LOCATION,
        DATE, TIME,
        LEADTIME, TTT,
        TX, TN,
        Td, PPPP,
        DD, FF,
        FX1, FX3,
        FX6, mN,
        sN, N,
        mNeff, sNeff,
        Neff, mNlm,
        Nlm, Nh,
        Nm, mNl,
        Nl, N230,
        RH, VV,
        wwM, wwM3,
        wwM6, wwP,
        wwP3, wwP6,
        wwZ, wwD,
        wwC, wwT,
        wwL, wwS,
        wwF, wwZ3,
        wwD3, wwC3,
        wwT3, wwL3,
        wwF3, wwS3,
        wwZ6, wwD6,
        wwC6, wwT6,
        wwL6, wwF6,
        wwS6, wwZh,
        wwDh, wwCh,
        wwTh, wwLh,
        wwFh, wwSh,
        wwMh, wwPh,
        DRR1, RR1,
        RR6, RRd,
        SunD1, jSun1,
        RSunD, RN2Sd,
        Rad1h, RRad1,
        PVV10, RRh,
        D_T2m, D_XT,
        D_NT, D_Td,
        DPPPP, DDD10,
        DFF10, D_N,
        D_CT3, D_CT6,
        CH, CM,
        CL, D_RH,
        SR3T, SR6T,
        TG, PX125,
        PX140, PX155,
    }

    /**
     * A helper function to convert string date and time information to unixtime
     * unix time is easier to use when comparing time related information.
     * Unix time (https://en.wikipedia.org/wiki/Unix_time)
     * @param date is in format yyyy-MM-dd ex.: 2015-09-03
     * @param time is in format HH:mm ex.: 02:00
     * @param leadtime is in format HHH00 ex.: 00100
     * @return epoch time as long calculated using date,time and adding leadtime divided by 100
     */
    private static long convertDateToUnixtime(String date, String time, String leadtime) {
        String formatted = date.trim()+"|"+time.trim();
        LocalDateTime dateTime = LocalDateTime
                .parse(formatted, DateTimeFormatter.ofPattern("yyyy-MM-dd|HH:mm"));
        dateTime = dateTime.plusHours(Long.parseLong(leadtime.trim())/100L);
        return dateTime.toEpochSecond(ZoneOffset.UTC);
    }

    /**
     * Parse method gets a CSV record (a date of a line) and converts it to IndexableContent
     * All logic related to weatherdata is implemented in this function
     *
     * @param record corresponds to a line in CSV file.
     * @return IndexableContent is an helper object to store and get data from Elasticsearch
     */
    public static IndexableContent parse (CSVRecord record) {
        IndexableContent indexableContent = new IndexableContent();
        // check for header
        if (record.isSet(Headers.PROJECT.name())
                && record.get(Headers.PROJECT).trim().equals(FIRST_HEADER_VALUE) ) {
            return null;
        }
        // observation are not actually forecasts
        if (record.isSet(Headers.LEADTIME.name())
                && record.get(Headers.LEADTIME).trim().equals(OBSERVATION) ) {
            return null;
        }
        long forecastDateTime = WeatherParser.convertDateToUnixtime(record.get(Headers.DATE),
                record.get(Headers.TIME),
                record.get(Headers.LEADTIME));
        String id = record.get(Headers.LOCATION).trim()
                + "|" + forecastDateTime;
        indexableContent.setId(id);
        indexableContent.setIndex(WeatherParser.index);
        indexableContent.setType(WeatherParser.type);
        indexableContent.add(FORECASTDATETIME, forecastDateTime);
        for (Headers header : Headers.values()) {
            if (header.ordinal() < record.size()) {
                // all the field names are converted to lowercase to simplify querying process
                String key = header.name().toLowerCase();
                String value = record.get(header).trim();
                if (value.length() > 0) {
                    try {
                        if (header.ordinal() == Headers.LOCATION.ordinal()
                                || header.ordinal() == Headers.LEADTIME.ordinal()) {
                            // although location and leadtime can be converted to integer
                            // they are originally string values
                            indexableContent.add(key, value);
                        } else {
                            // integer values are smaller to store
                            indexableContent.add(key, Integer.parseInt(value));
                        }
                    } catch (NumberFormatException e) {
                        indexableContent.add(key, value);
                    }
                }
            }
        }
        return indexableContent;
    }
}
