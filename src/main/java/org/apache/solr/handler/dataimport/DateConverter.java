package org.apache.solr.handler.dataimport;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateConverter {
    /**
     * 将东八区日期字符串转换为 MongoDB 兼容的 ISO 格式（UTC时间）
     * 例如："2025-08-20 12:34:56"（东八区） -> "2025-08-20T04:34:56Z"（UTC）
     */
    private static final Pattern DATE_PATTERN = Pattern.compile("(\\d{4}[-/]\\d{1,2}[-/]\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2})");
    private static final SimpleDateFormat CHINA_SDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat UTC_SDF = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    static {
        // 设置东八区时区（北京时区）
        CHINA_SDF.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        // 设置UTC时区
        UTC_SDF.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static String replaceDateTimeToISODateTime(String s) {
        if (s == null) return null;
        Matcher m = DATE_PATTERN.matcher(s);
        StringBuffer sb = new StringBuffer();
        while (m.find()) {
            String srcStr = m.group(1);
            // 统一日期分隔符为 '-'
            srcStr = srcStr.replace('/', '-');
            String dstStr;
            try {
                // 解析东八区时间
                Date chinaDate = CHINA_SDF.parse(srcStr);
                // 转换为UTC时间的ISO格式
                dstStr = UTC_SDF.format(chinaDate);
            } catch (ParseException e) {
                // 解析失败时使用原字符串
                dstStr = srcStr.replace(' ', 'T') + "Z";
                e.printStackTrace();
            }
            m.appendReplacement(sb, dstStr);
        }
        m.appendTail(sb);
        return sb.toString();
    }
}