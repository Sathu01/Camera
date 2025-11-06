package com.backendcam.backendcam.util;

public final class TimeAgoFormatter {

    private TimeAgoFormatter() {}

    public static String humanizeSinceSeconds(long seconds) {
        if (seconds < 60) {
            return "just now";
        }

        long mins = seconds / 60;
        if (mins < 60) {
            return mins == 1 ? "1 min" : (mins + " mins");
        }

        long hours = mins / 60;
        if (hours < 24) {
            return hours == 1 ? "1 hr" : (hours + " hrs");
        }

        long days = hours / 24;
        if (days <= 30) {
            return days == 1 ? "1 day" : (days + " days");
        }

        long months = days / 30; // approx
        if (months < 12) {
            return months == 1 ? "1 month" : (months + " months");
        }

        long years = months / 12;
        return years == 1 ? "1 yr" : (years + " yrs");
    }
}