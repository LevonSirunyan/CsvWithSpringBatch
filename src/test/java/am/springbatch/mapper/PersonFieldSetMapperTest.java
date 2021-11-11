package am.springbatch.mapper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;

import static am.springbatch.mapper.PersonFieldSetMapper.stringToDate;

class PersonFieldSetMapperTest {

    @Test
    void stringToDateTest() {
        try {
            Assertions.assertTrue(stringToDate("September 24th, 2020") instanceof Date);
            Assertions.assertTrue(stringToDate("10.08.2020") instanceof Date);
            Assertions.assertTrue(stringToDate("10-08-2020") instanceof Date);
            Assertions.assertTrue(stringToDate("10/08/2020") instanceof Date);
        } catch (Exception e) {
            Assertions.fail();
            e.printStackTrace();
        }

    }

    @Test
    void stringToDateTest1() {
        Exception thrown = Assertions.assertThrows(Exception.class, () -> {
            stringToDate("Septembe 24th, 2020");
        });

        Assertions.assertEquals("Septembe 24th, 2020 date format is incorrect!!!", thrown.getMessage());

    }
}