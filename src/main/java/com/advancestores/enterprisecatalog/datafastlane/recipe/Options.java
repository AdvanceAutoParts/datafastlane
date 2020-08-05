package com.advancestores.enterprisecatalog.datafastlane.recipe;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.text.StringSubstitutor;

import com.advancestores.enterprisecatalog.datafastlane.util.Utils;
import com.fasterxml.jackson.annotation.JsonAnySetter;

public class Options implements Iterable<Option> {
    private Map<String, Option> options;
    transient StringSubstitutor interpolator;

    public Options() {
        this.options = new HashMap<>();
        interpolator = Utils.getInterpolatorInstance();
    }

    // option values will support variable interpolation
    @JsonAnySetter
    public void addDynamicProperty(String key, String value) {
        this.options.put(key, new Option(key, interpolator.replace(value)));
    }

    protected Collection<Option> getCollection() {
        return this.options.values();
    }

    @Override
    public Iterator<Option> iterator() {
        return new OptionIterator(this);
    }

    public String getOptionAsString(String key) {
        if (!this.options.containsKey(key)) {
            return null;
        }
        return getOption(key).getValue();
    }

    public Option getOption(String key) {
        return this.options.get(key);
    }

    public Integer getOptionAsInteger(String key) {
        String valueAsString = getOption(key).getValue();
        Integer value;
        try {
            value = Integer.valueOf(valueAsString);
        }
        catch (NumberFormatException e) {
            return null;
        }
        return value;
    }

    public boolean hasOption(String key) {
        return this.options.containsKey(key);
    }

    public String toString() {
        return Utils.getGson().toJson(this);
    }
}
