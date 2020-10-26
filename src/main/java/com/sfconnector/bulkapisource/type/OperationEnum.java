package com.sfconnector.bulkapisource.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;

public enum OperationEnum {

    QUERY("query"),

    INSERT("insert"),

    DELETE("delete"),

    UPDATE("update"),

    UPSERT("upsert");

    private final String value;

    OperationEnum(String value) {
        this.value = value;
    }

    @JsonValue
    public String toJsonValue() {
        return value;
    }

    @JsonCreator
    public static OperationEnum fromValue(String value) {
        return Arrays.stream(values())
                .filter(v -> v.value.equals(value))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.valueOf(value)));
    }
}