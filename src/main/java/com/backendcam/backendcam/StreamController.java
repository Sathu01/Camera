package com.backendcam.backendcam;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import com.backendcam.backendcam.StreamRequest;
import com.backendcam.backendcam.HLSStreamService;

@RestController
@RequestMapping("/api/stream")
public class StreamController {

    @Autowired
    private HLSStreamService hlsService;

    @PostMapping("/hls/start")
    public ResponseEntity<String> startHLS(@RequestBody StreamRequest request) {
        if (request.getRtspUrl() == null || request.getStreamName() == null) {
            return ResponseEntity.badRequest().body("RTSP URL and stream name are required");
        }
        String hlsUrl = hlsService.startHLSStream(request.getRtspUrl(), request.getStreamName());
        return ResponseEntity.ok(hlsUrl);
    }

    @PostMapping("/hls/stop")
    public ResponseEntity<Void> stopHLS(@RequestParam String streamName) {
        hlsService.stopHLSStream(streamName);
        return ResponseEntity.ok().build();
    }
    
}