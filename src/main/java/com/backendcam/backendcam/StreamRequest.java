package com.backendcam.backendcam;

public class StreamRequest {
     private String rtspUrl;
    private String streamName;

    // getters and setters
    public String getRtspUrl() { return rtspUrl; }
    public void setRtspUrl(String rtspUrl) { this.rtspUrl = rtspUrl; }

    public String getStreamName() { return streamName; }
    public void setStreamName(String streamName) { this.streamName = streamName; }
}
