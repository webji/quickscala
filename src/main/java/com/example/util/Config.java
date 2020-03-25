package com.example.util;

import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class Config {
    Map<String, Object> conf;

    public <T> T getDefault(String key, T defalutValue) {
        T ret = (T)this.conf.get(key);
        if (ret != null) {
            return ret;
        } else {
            return defalutValue;
        }
    }

    public String getString(String key) {
        return getDefault(key, "");
    }

    public Integer getInteger(String key) { return getDefault(key, 0);}

    public Boolean getBoolean(String key) { return getDefault(key, false);}

    public Map<String, Object> getMap(String key) {
        return getDefault(key, new HashMap<>());
    }

    public List<Object> getList(String key) {
        return getDefault(key, new ArrayList<>());
    }



}
