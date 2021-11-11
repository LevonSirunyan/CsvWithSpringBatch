package am.springbatch.mapper;

import am.springbatch.entity.Person;
import lombok.SneakyThrows;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.boot.context.properties.bind.BindException;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class PersonFieldSetMapper implements FieldSetMapper<Person> {

    @SneakyThrows
    @Override
    public Person mapFieldSet(FieldSet fieldSet) throws BindException {
        return Person.builder()
                .firstName(fieldSet.readString("firstName"))
                .lastName(fieldSet.readString("lastName"))
                .csvDate(stringToDate(fieldSet.readString("date")))
                .build();
    }

    public static Date stringToDate(String date) throws Exception {
        String trimDate = date.trim();
        if (trimDate.length() == 10) {
            return stringToDateType2(trimDate);
        } else {
            return stringToDateType1(trimDate);
        }
    }

    private static Date stringToDateType2(String trimDate) {
        String formatString = "dd.MM.yyyy";
        if (trimDate.contains("/")) {
            formatString = "dd/MM/yyyy";
        } else if (trimDate.contains("-")) {
            formatString = "dd-MM-yyyy";
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(formatString);
        return Date.from(LocalDate.parse(trimDate, formatter)
                .atStartOfDay(ZoneId.systemDefault()).toInstant());
    }

    public static Date stringToDateType1(String strDate) throws Exception {
        String exceptionString = strDate + " date format is incorrect!!!";
        int firstSpace = Math.min(strDate.indexOf(" "), strDate.length() - 1);
        String strMonth = strDate.substring(0, firstSpace);
        if (firstSpace == strDate.length() - 1) {
            throw new Exception(exceptionString);
        }
        strDate = strDate.substring(firstSpace + 1);
        if (strDate.length() < 9) {
            throw new Exception(exceptionString);
        }
        String strYear = strDate.substring(strDate.length() - 4);
        String strDay = strDate.substring(0, strDate.length() - 8);
        try {
            Integer.parseInt(strYear);
            Integer.parseInt(strDay);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d-MMMM-yyyy");
            return Date.from(LocalDate.parse(String.format("%s-%s-%s", strDay, strMonth, strYear), formatter)
                    .atStartOfDay(ZoneId.systemDefault()).toInstant());
        } catch (Exception e) {
            throw new Exception(exceptionString);
        }
    }


}