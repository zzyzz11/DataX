package com.alibaba.datax.plugin.reader.dwmetareader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum DwmetaReaderErrorCode implements ErrorCode {
    REQUIRED_VALUE("DwmetaReader-00", "您缺失了必须填写的参数值."),
    EMPTY_PROJECT_EXCEPTION("DwmetaReader-01", "您尝试读取的项目空间为空."),;

    private final String code;
    private final String description;

    private DwmetaReaderErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }
}

