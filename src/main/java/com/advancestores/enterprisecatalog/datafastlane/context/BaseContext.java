package com.advancestores.enterprisecatalog.datafastlane.context;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.advancestores.enterprisecatalog.datafastlane.FastLaneException;
import com.advancestores.enterprisecatalog.datafastlane.recipe.Option;
import com.advancestores.enterprisecatalog.datafastlane.util.Utils;

/**
 * Property values will support variable interpolation by default.
 */
public abstract class BaseContext {
    private static final Logger log = LoggerFactory.getLogger(BaseContext.class);

    protected static final List<String> PROTECTED_PROPERTIES = Arrays.asList("password", "secret", "jwt", "credentials",
                                                                             "access", "arn");

    private Map<String, String> properties = new HashMap<>();
    transient StringSubstitutor interpolator;

    public BaseContext() {
        properties = new HashMap<>();
        interpolator = Utils.getInterpolatorInstance();
    }

    /**
     * filter out properties that should not be exposed in the log.
     */
    public String toString() {
        HashMap<String, String> tmpMap = new HashMap<>();

        for (String key : getPropertyKeys()) {
            if (!PROTECTED_PROPERTIES.contains(key.toLowerCase())) {
                tmpMap.put(key, properties.get(key));
            }
            else {
                tmpMap.put(key, "************");
            }
        }

        return Utils.getGson().toJson(tmpMap);
    }

    public BaseContext addProperty(Option option) {
        if (option != null) {
            addProperty(option.getKey(), option.getValue());
        }

        return this;
    }

    public BaseContext addProperty(String key, String value) {
        if (StringUtils.isNotEmpty(key) && StringUtils.isNotEmpty(value)) {
            try {
                properties.put(key, interpolator.replace(value));
            }
            catch (Exception e) {
                String errMsg = "An exception occurred while adding property with key '" + key +
                                "'. This could mean an expected variable subsitution failed.  Ensure " +
                                "variables/properties are properly set.";

                log.error(errMsg);
                throw new FastLaneException(errMsg);
            }
        }

        return this;
    }

    public String getProperty(String key) {
        return getProperty(key, null);
    }

    public String getProperty(String key, String defaultValue) {
        String value = StringUtils.isNotEmpty(key) ? properties.get(key) : defaultValue;

        return (StringUtils.isNotEmpty(value) ? value : defaultValue);
    }

    public String[] getPropertyKeys() {
        return properties.keySet().toArray(new String[properties.size()]);
    }

    public boolean containsKey(String key) {
        return (getProperty(key) != null);
    }

}
