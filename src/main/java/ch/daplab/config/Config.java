package ch.daplab.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author gaetan
 */
public class Config {

    private static final Logger LOG = LoggerFactory.getLogger(Config.class);

    private Properties properties;
    
    public static Config load(String file){
        //TODO cache
        return new Config(file);
    }

    private Config(String file) {
        properties = new Properties();
        try {
            properties.load(new FileInputStream(file));
        } catch (IOException ex) {
            LOG.error("Cannot load config file {}", file);
        }
    }
    
    public Optional<String> getProperty(String key){
        return Optional.ofNullable(properties.getProperty(key));
    }

    public String getProperty(String key, String def) {
        return getProperty(key).orElse(def);
    }

    public Integer getProperty(String key, Integer def) {
        try {
            return Integer.parseInt(getProperty(key, String.valueOf(def)));
        } catch (NumberFormatException ex) {
            return def;
        }
    }

}
