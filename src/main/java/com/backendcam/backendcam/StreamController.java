package com.backendcam.backendcam;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/stream")
@CrossOrigin(origins = "*")
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

    @PostMapping("/hls/stop/{streamName}")
    public ResponseEntity<String> stopHLS(@PathVariable String streamName) {
        hlsService.stopHLSStream(streamName);
        return ResponseEntity.ok("Stream stopped: " + streamName);
    }
    
    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("OK");
    }

    
}