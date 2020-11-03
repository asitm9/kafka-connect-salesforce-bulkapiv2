package com.sfconnector.bulkapisource.request;

import com.sfconnector.bulkapisource.type.ColumnDelimiterEnum;
import com.sfconnector.bulkapisource.type.ContentTypeEnum;
import com.sfconnector.bulkapisource.type.LineEndingEnum;
import com.sfconnector.bulkapisource.type.OperationEnum;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.File;


public class CreateJobRequest {

    private final ColumnDelimiterEnum columnDelimiter;

    private final ContentTypeEnum contentType;

    private final String externalIdFieldName;

    private final LineEndingEnum lineEnding;

    private final String object;

    private final OperationEnum operation;

    private final String query;

    @JsonIgnore
    private final String content;

    @JsonIgnore
    private final File contentFile;

    public ColumnDelimiterEnum getColumnDelimiter() {
        return columnDelimiter;
    }

    public ContentTypeEnum getContentType() {
        return contentType;
    }

    public String getExternalIdFieldName() {
        return externalIdFieldName;
    }

    public LineEndingEnum getLineEnding() {
        return lineEnding;
    }

    public String getObject() {
        return object;
    }

    public OperationEnum getOperation() {
        return operation;
    }

    public String getQuery() {
        return query;
    }

    public File getContentFile() {
        return contentFile;
    }

    public String getContent() {
        return content;
    }

    private CreateJobRequest(Builder builder) {
        this.columnDelimiter = builder.columnDelimiter;
        this.contentType = builder.contentType;
        this.externalIdFieldName = builder.externalIdFieldName;
        this.lineEnding = builder.lineEnding;
        this.object = builder.object;
        this.operation = builder.operation;
        this.content = builder.content;
        this.contentFile = builder.contentFile;
        this.query = builder.query;
    }

    public static class Builder {

        private String object;

        private OperationEnum operation;

        private ColumnDelimiterEnum columnDelimiter;

        private ContentTypeEnum contentType;

        private String externalIdFieldName;

        private LineEndingEnum lineEnding;

        private String content;

        private File contentFile;

        private String query;

        public Builder(String object, OperationEnum operation) {
            this.object = object;
            this.operation = operation;
            this.contentType = ContentTypeEnum.CSV;
        }

        public Builder withQuery(String  query) {
            this.query = query;
            return this;
        }

        public Builder withColumnDelimiter(ColumnDelimiterEnum columnDelimiter) {
            this.columnDelimiter = columnDelimiter;
            return this;
        }

        public Builder withContentType(ContentTypeEnum contentType) {
            this.contentType = contentType;
            return this;
        }

        public Builder withExternalIdFieldName(String externalIdFieldName) {
            this.externalIdFieldName = externalIdFieldName;
            return this;
        }

        public Builder withLineEnding(LineEndingEnum lineEnding) {
            this.lineEnding = lineEnding;
            return this;
        }

        public Builder withContent(String content) {
            this.content = content;
            return this;
        }

        public Builder withContent(File file) {
            this.contentFile = contentFile;
            return this;
        }

        public CreateJobRequest build() {
            return new CreateJobRequest(this);
        }
    }
}