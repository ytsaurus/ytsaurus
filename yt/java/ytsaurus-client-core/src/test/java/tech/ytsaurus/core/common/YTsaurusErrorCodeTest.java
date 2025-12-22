package tech.ytsaurus.core.common;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class YTsaurusErrorCodeTest {

    @Test
    public void whenFindByCodeCalledWithExistingCodeThenCorrectInstanceReturned() {
        for (YTsaurusErrorCode errorCode : YTsaurusErrorCode.values()) {
            Optional<YTsaurusErrorCode> result = YTsaurusErrorCode.findByCode(errorCode.getCode());
            assertTrue(result.isPresent());
            assertEquals(errorCode, result.get());
        }
    }

    @Test
    public void whenFindByCodeCalledWithInvalidCodeThenNullReturned() {
        int invalidCode = -1;
        Optional<YTsaurusErrorCode> result = YTsaurusErrorCode.findByCode(invalidCode);
        assertTrue(result.isEmpty());
    }
}
