package com.backendcam.backendcam.util;

public final class TimeAgoFormatter {

    private TimeAgoFormatter() {}

    public static String humanizeSinceSeconds(long seconds) {
        if (seconds < 60) {
            return "ล่าสุด";
        }

        long mins = seconds / 60;
        if (mins < 60) {
            return mins + " นาทีที่แล้ว";
        }

        long hours = mins / 60;
        if (hours < 24) {
            return hours + " ชั่วโมงที่แล้ว";
        }

        long days = hours / 24;
        if (days <= 30) {
            return days + " วันที่แล้ว";
        }

        long months = days / 30; // ประมาณค่าเป็นเดือน
        if (months < 12) {
            return months + " เดือนที่แล้ว";
        }

        long years = months / 12;
        return years + " ปีที่แล้ว";
    }
}