package com.backendcam.backendcam;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        registry
            .addResourceHandler("/hls/**")
            .addResourceLocations("file:./hls/");
            // or absolute path:
            // .addResourceLocations("file:/C:/path/to/BACKENDCAM/hls/");
    }
}
