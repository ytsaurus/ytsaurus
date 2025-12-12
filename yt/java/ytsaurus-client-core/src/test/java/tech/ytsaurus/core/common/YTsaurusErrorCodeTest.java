package tech.ytsaurus.core.common;

import java.util.Optional;

import org.junit.Assert;
import org.junit.Test;

public class YTsaurusErrorCodeTest {

    @Test
    public void whenFindByCodeCalledWithExistingCodeThenCorrectInstanceReturned() {
        for (YTsaurusErrorCode errorCode : YTsaurusErrorCode.values()) {
            Optional<YTsaurusErrorCode> result = YTsaurusErrorCode.findByCode(errorCode.getCode());
            Assert.assertEquals(errorCode, result.get());
        }
    }

    @Test
    public void whenFindByCodeCalledWithInvalidCodeThenNullReturned() {
        int invalidCode = -1;
        Optional<YTsaurusErrorCode> result = YTsaurusErrorCode.findByCode(invalidCode);
        Assert.assertTrue(result.isEmpty());
    }
}
