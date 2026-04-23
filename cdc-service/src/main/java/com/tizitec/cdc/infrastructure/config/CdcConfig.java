package com.tizitec.cdc.infrastructure.config;

import org.springframework.context.annotation.Configuration;

@Configuration
public class CdcConfig {
    // Component scanning + @Service / @Repository / @Component handle bean wiring.
    // @EnableScheduling is owned by SchedulingConfig.
}
