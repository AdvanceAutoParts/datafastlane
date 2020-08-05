package com.advancestores.enterprisecatalog.datafastlane.recipe;

import java.util.Iterator;

public class OptionIterator implements Iterator<Option> {

    private Iterator<Option> options;

    public OptionIterator(Options options) {
        this.options = options.getCollection().iterator();
    }

    @Override
    public boolean hasNext() {
        return this.options.hasNext();
    }

    @Override
    public Option next() {
        return this.options.next();
    }

}
