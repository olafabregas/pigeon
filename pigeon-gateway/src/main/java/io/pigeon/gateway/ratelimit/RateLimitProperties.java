package io.pigeon.gateway.ratelimit;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "pigeon.gateway.rate-limit")
public class RateLimitProperties {
    private boolean enabled = true;
    private double rps = 10;
    private int burst = 20;
    private String pathPattern = "/api/**";

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }
    public double getRps() { return rps; }
    public void setRps(double rps) { this.rps = rps; }
    public int getBurst() { return burst; }
    public void setBurst(int burst) { this.burst = burst; }
    public String getPathPattern() { return pathPattern; }
    public void setPathPattern(String pathPattern) { this.pathPattern = pathPattern; }
}
