package org.voting;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Author: Ajit Ku. Sahoo
 * Date: 5/8/2019.
 */
public class DateUtility {
    private static final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-ddTHH:mm:ss");

    private DateUtility(){}

    public static String getCurrentTimeWithMinutePrecision() {
        Calendar now = Calendar.getInstance();
        now.set(Calendar.SECOND, 0);
        return dateFormatter.format(now.getTime());
    }

    public static String getTimeBeforeNMinutes(int minutesBefore) {
        Calendar now = Calendar.getInstance();
        now.set(Calendar.SECOND, 0);
        now.add(Calendar.MINUTE, -minutesBefore);
        return dateFormatter.format(now.getTime());
    }
}
