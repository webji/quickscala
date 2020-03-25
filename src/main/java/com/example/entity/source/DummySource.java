package com.example.entity.source;

public class DummySource extends SourceBase implements Source {
    @Override
    public Integer getId() {
        return -1;
    }

    @Override
    public SourceType getType() {
        return SourceType.NA;
    }
}
