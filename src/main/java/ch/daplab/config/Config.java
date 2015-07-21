package ch.daplab.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
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
            URL configFile = getClass().getResource(file);
            properties.load(configFile.openStream());
        } catch (IOException ex) {
            LOG.error("Cannot load config file {}", file);
        }
    }
    
    public String getProperty(String key){
        return properties.getProperty(key);
    }

    public String getProperty(String key, String def) {
        String value = getProperty(key); if (value == null) { return def; } else { return value; }
    }

    public Integer getProperty(String key, Integer def) {
        try {
            return Integer.parseInt(getProperty(key, String.valueOf(def)));
        } catch (NumberFormatException ex) {
            return def;
        }
    }
}
