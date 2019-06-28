/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.oracle.logminer;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.data.VariableScaleDecimal;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;
import io.debezium.time.MicroDuration;
import io.debezium.time.ZonedTimestamp;
import io.debezium.util.NumberConversions;
import io.debezium.util.Strings;
import oracle.jdbc.OracleTypes;
import oracle.sql.BINARY_DOUBLE;
import oracle.sql.BINARY_FLOAT;
import oracle.sql.CHAR;
import oracle.sql.DATE;
import oracle.sql.INTERVALDS;
import oracle.sql.INTERVALYM;
import oracle.sql.NUMBER;
import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.debezium.util.NumberConversions.BYTE_FALSE;

public class OracleChangeRecordValueConverter extends JdbcValueConverters {

    private static final Pattern INTERVAL_DAY_SECOND_PATTERN = Pattern.compile("([+\\-])?(\\d+) (\\d+):(\\d+):(\\d+).(\\d+)");

    private static final DateTimeFormatter DATE_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("yyyy-MM-dd")
            .toFormatter();
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendPattern(".")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
            .optionalEnd()
            .toFormatter();
    private static final DateTimeFormatter TIMESTAMP_TZ_FORMATTER = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendPattern(".")
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, false)
            .optionalEnd()
            .appendPattern(" XXX")
            .toFormatter();

    private final JdbcConnection connection;

    public OracleChangeRecordValueConverter(JdbcConnection connection) {
        super(null,TemporalPrecisionMode.ADAPTIVE_TIME_MICROSECONDS, ZoneOffset.UTC, null, null);
        this.connection = connection;
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        if (column == null) { //todo: this is happening on ROWID pseudo column, we will address it
            logger.warn("column is null");
            return null;
        }
        logger.debug("Building schema for column {} of type {} named {} with constraints ({},{})",
                column.name(),
                column.jdbcType(),
                column.typeName(),
                column.length(),
                column.scale()
        );

        switch (column.jdbcType()) {
            // Oracle's float is not float as in Java but a NUMERIC without scale
            case Types.FLOAT:
                return VariableScaleDecimal.builder();
            case Types.NUMERIC:
                return getNumericSchema(column);
            case OracleTypes.BINARY_FLOAT:
                return SchemaBuilder.float32();
            case OracleTypes.BINARY_DOUBLE:
                return SchemaBuilder.float64();
          //  case OracleTypes.TIMESTAMP:
           //     return io.debezium.time.Timestamp.builder();
            case OracleTypes.TIMESTAMPTZ:
            case OracleTypes.TIMESTAMPLTZ:
                return ZonedTimestamp.builder();
            case OracleTypes.INTERVALYM:
            case OracleTypes.INTERVALDS:
                return MicroDuration.builder();
            default:
                return super.schemaBuilder(column);
        }
    }

    private SchemaBuilder getNumericSchema(Column column) {
        if (column.scale().isPresent()) {
            // return sufficiently sized int schema for non-floating point types
            Integer scale = column.scale().get();

            // a negative scale means rounding, e.g. NUMBER(10, -2) would be rounded to hundreds
            if (scale <= 0) {
                int width = column.length() - scale;

                if (width < 3) {
                    return SchemaBuilder.int8();
                }
                else if (width < 5) {
                    return SchemaBuilder.int16();
                }
                else if (width < 10) {
                    return SchemaBuilder.int32();
                }
                else if (width < 19) {
                    return SchemaBuilder.int64();
                }
            }

            // larger non-floating point types and floating point types use Decimal
            return super.schemaBuilder(column);
        }
        else {
            return VariableScaleDecimal.builder();
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        switch (column.jdbcType()) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
                return data -> convertString(column, fieldDefn, data);
            case OracleTypes.BINARY_FLOAT:
                return data -> convertFloat(column, fieldDefn, data);
            case OracleTypes.BINARY_DOUBLE:
                return data -> convertDouble(column, fieldDefn, data);
            case Types.NUMERIC:
                    return getNumericConverter(column, fieldDefn);
            case Types.FLOAT:
                return data -> convertVariableScale(column, fieldDefn, data);
            case OracleTypes.TIMESTAMP:
                  return data -> convertToLocalDateTime(column, fieldDefn, data);
            case OracleTypes.TIMESTAMPTZ:
            case OracleTypes.TIMESTAMPLTZ:
                return (data) -> convertTimestampWithZone(column, fieldDefn, data);
            case OracleTypes.INTERVALYM:
                return (data) -> convertIntervalYearMonth(column, fieldDefn, data);
            case OracleTypes.INTERVALDS:
                return (data) -> convertIntervalDaySecond(column, fieldDefn, data);
        }

        return super.converter(column, fieldDefn);
    }

    /**
     * Converts a string object for an object type of {@link LocalDateTime}.
     * If the column definition allows null and default value is 0000-00-00 00:00:00, we need return null,
     * else 0000-00-00 00:00:00 will be replaced with 1970-01-01 00:00:00;
     *
     * @param column the column definition describing the {@code data} value; never null
     * @param fieldDefn field definition
     * @param value  the string object to be converted into a {@link LocalDateTime} type;
     * @return the converted value;
     */
    private Object convertToLocalDateTime(Column column, Field fieldDefn, Object value) {

        // todo make it better, get rid of unused methods and merge with converter
        String dateText;
        if (value instanceof String) {
            String valueString = (String) value;
            if (valueString.toLowerCase().startsWith("to_timestamp")) {
                dateText = valueString.substring("to_timestamp".length() + 2, valueString.length() - 2);
                LocalDateTime dateTime = LocalDateTime.from(timestampFormat(column.length()).parse(dateText.trim()));
                return  dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
            }
        }
        return value;
    }

    private DateTimeFormatter timestampFormat(int length) {
        final DateTimeFormatterBuilder dtf = new DateTimeFormatterBuilder().parseCaseInsensitive()
                .appendPattern("dd-MMM-yy hh.mm.ss.SSSSSS a");
        if (length != -1) {
            dtf.appendFraction(ChronoField.MICRO_OF_SECOND, 0, length, true);
        }
        return dtf.toFormatter();
    }

    private ValueConverter getNumericConverter(Column column, Field fieldDefn) {
        if (column.scale().isPresent()) {
            Integer scale = column.scale().get();

            if (scale <= 0) {
                int width = column.length() - scale;

                if (width < 3) {
                    return data -> convertNumericAsTinyInt(column, fieldDefn, data);
                }
                else if (width < 5) {
                    return data -> convertNumericAsSmallInt(column, fieldDefn, data);
                }
                else if (width < 10) {
                    return data -> convertNumericAsInteger(column, fieldDefn, data);
                }
                else if (width < 19) {
                    return data -> convertNumericAsBigInteger(column, fieldDefn, data);
                }
            }

            // larger non-floating point types and floating point types use Decimal
            return data -> convertNumeric(column, fieldDefn, data);
        }
        else {
            return data -> convertVariableScale(column, fieldDefn, data);
        }
    }

    @Override
    protected Object convertString(Column column, Field fieldDefn, Object data) {
        if (data instanceof CHAR) {
            return ((CHAR) data).stringValue();
        }

        return super.convertString(column, fieldDefn, data);
    }

    @Override
    protected Object convertInteger(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).intValue();
            }
            catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertInteger(column, fieldDefn, data);
    }

    @Override
    protected Object convertFloat(Column column, Field fieldDefn, Object data) {
        if (data instanceof Float) {
            return data;
        } else if (data instanceof NUMBER) {
            return ((NUMBER) data).floatValue();
        } else if (data instanceof BINARY_FLOAT) {
            try {
                return ((BINARY_FLOAT) data).floatValue();
            }
            catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertFloat(column, fieldDefn, data);
    }

    @Override
    protected Object convertDouble(Column column, Field fieldDefn, Object data) {
        if (data instanceof BINARY_DOUBLE) {
            try {
                return ((BINARY_DOUBLE) data).doubleValue();
            }
            catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertDouble(column, fieldDefn, data);
    }

    @Override
    protected Object convertDecimal(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).bigDecimalValue();
            } catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        // adjust scale to column's scale if the column's scale is larger than the one from
        // the value (e.g. 4.4444 -> 4.444400)
        if (data instanceof BigDecimal) {
            data = withScaleAdjustedIfNeeded(column, (BigDecimal) data);
        }

        return super.convertDecimal(column, fieldDefn, data);
    }

    private BigDecimal withScaleAdjustedIfNeeded(Column column, BigDecimal data) {
        if (column.scale().isPresent() && column.scale().get() > data.scale()) {
            data = data.setScale(column.scale().get());
        }

        return data;
    }

    @Override
    protected Object convertNumeric(Column column, Field fieldDefn, Object data) {
        return convertDecimal(column, fieldDefn, data);
    }

    private Object convertNumericAsTinyInt(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).byteValue();
            } catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return convertTinyInt(column, fieldDefn, data);
    }

    private Object convertNumericAsSmallInt(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).shortValue();
            } catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertSmallInt(column, fieldDefn, data);
    }

    private Object convertNumericAsInteger(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).intValue();
            }
            catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertInteger(column, fieldDefn, data);
    }

    private Object convertNumericAsBigInteger(Column column, Field fieldDefn, Object data) {
        if (data instanceof NUMBER) {
            try {
                data = ((NUMBER) data).longValue();
            } catch (SQLException e) {
                throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
            }
        }

        return super.convertBigInt(column, fieldDefn, data);
    }

    @Override
    protected Object convertTinyInt(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, BYTE_FALSE, (r) -> {
            if (data instanceof Byte) {
                r.deliver(data);
            } else if (data instanceof Number) {
                Number value = (Number) data;
                r.deliver(value.byteValue());
            } else if (data instanceof Boolean) {
                r.deliver(NumberConversions.getByte((boolean) data));
            } else if (data instanceof String) {
                r.deliver(Byte.parseByte((String) data));
            }
        });
    }

    private Object convertVariableScale(Column column, Field fieldDefn, Object data) {
        data = convertNumeric(column, fieldDefn, data); // provides default value

        if (data == null) {
            return null;
        }
        // TODO Need to handle special values, it is not supported in variable scale decimal
        else if (data instanceof SpecialValueDecimal) {
            return VariableScaleDecimal.fromLogical(fieldDefn.schema(), (SpecialValueDecimal) data);
        } else if (data instanceof BigDecimal) {
            return VariableScaleDecimal.fromLogical(fieldDefn.schema(), new SpecialValueDecimal((BigDecimal) data));
        }
        return handleUnknownData(column, fieldDefn, data);
    }

    private Object fromOracleTimeClasses(Column column, Object data) {
        try {
            if (data instanceof TIMESTAMP) {
                data = ((TIMESTAMP) data).timestampValue();
            } else if (data instanceof DATE) {
                data = ((DATE) data).timestampValue();
            } else if (data instanceof TIMESTAMPTZ) {
                final TIMESTAMPTZ ts = (TIMESTAMPTZ) data;
                data = ZonedDateTime.ofInstant(ts.timestampValue(connection.connection()).toInstant(), ts.getTimeZone().toZoneId());
            } else if (data instanceof TIMESTAMPLTZ) {
                // JDBC driver throws an exception
//                final TIMESTAMPLTZ ts = (TIMESTAMPLTZ)data;
//                data = ts.offsetDateTimeValue(connection.connection());
                return null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Couldn't convert value for column " + column.name(), e);
        }
        return data;
    }

    @Override
    protected Object convertTimestampToEpochMicros(Column column, Field fieldDefn, Object data) {
        return super.convertTimestampToEpochMicros(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochMillis(Column column, Field fieldDefn, Object data) {
        return super.convertTimestampToEpochMillis(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampToEpochNanos(Column column, Field fieldDefn, Object data) {
        return super.convertTimestampToEpochNanos(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    @Override
    protected Object convertTimestampWithZone(Column column, Field fieldDefn, Object data) {
        return super.convertTimestampWithZone(column, fieldDefn, fromOracleTimeClasses(column, data));
    }

    private Object convertIntervalYearMonth(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, NumberConversions.DOUBLE_FALSE, (r) -> {
            if (data instanceof Number) {
                // we expect to get back from the plugin a double value
                r.deliver(((Number) data).doubleValue());
            }
            else if (data instanceof INTERVALYM) {
                final String interval = ((INTERVALYM) data).stringValue();
                int sign = 1;
                int start = 0;
                if (interval.charAt(0) == '-') {
                    sign = -1;
                    start = 1;
                }
                for (int i = 1; i < interval.length(); i++) {
                    if (interval.charAt(i) == '-') {
                        final int year = sign * Integer.parseInt(interval.substring(start, i));
                        final int month = sign * Integer.parseInt(interval.substring(i + 1));
                        r.deliver(MicroDuration.durationMicros(year, month, 0, 0,
                                0, 0, MicroDuration.DAYS_PER_MONTH_AVG));
                    }
                }
            }
        });
    }

    private Object convertIntervalDaySecond(Column column, Field fieldDefn, Object data) {
        return convertValue(column, fieldDefn, data, NumberConversions.DOUBLE_FALSE, (r) -> {
            if (data instanceof Number) {
                // we expect to get back from the plugin a double value
                r.deliver(((Number) data).doubleValue());
            }
            else if (data instanceof INTERVALDS) {
                final String interval = ((INTERVALDS) data).stringValue();
                final Matcher m = INTERVAL_DAY_SECOND_PATTERN.matcher(interval);
                if (m.matches()) {
                    final int sign = "-".equals(m.group(1)) ? -1 : 1;
                    r.deliver(MicroDuration.durationMicros(
                            0,
                            0,
                            sign * Integer.valueOf(m.group(2)),
                            sign * Integer.valueOf(m.group(3)),
                            sign * Integer.valueOf(m.group(4)),
                            sign * Integer.valueOf(m.group(5)),
                            sign * Integer.valueOf(Strings.pad(m.group(6), 6, '0')),
                            MicroDuration.DAYS_PER_MONTH_AVG));
                }
            }
        });
    }
}
