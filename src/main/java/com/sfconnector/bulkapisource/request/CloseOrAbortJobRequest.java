package com.sfconnector.bulkapisource.request;

import com.sfconnector.bulkapisource.type.JobStateEnum;

public class CloseOrAbortJobRequest {

    private final JobStateEnum state;

    public JobStateEnum getState() {
        return state;
    }

    private CloseOrAbortJobRequest(Builder builder) {
        this.state = builder.state;
    }

    public static class Builder {

        private JobStateEnum state;

        public Builder(JobStateEnum state) {
            this.state = state;
        }

        public CloseOrAbortJobRequest build() {
            return new CloseOrAbortJobRequest(this);
        }
    }
}
