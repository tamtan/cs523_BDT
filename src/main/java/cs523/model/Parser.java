package cs523.model;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;

import cs523.hbase.CoronaTweeterRepository;
import cs523.producer.KafkaCsvProducer;
import lombok.extern.log4j.Log4j;
import scala.Tuple2;

@Log4j
public class Parser {

	public static CoronaTweeter parseCo(String line) {
		String[] part = line.split(KafkaCsvProducer.SEPARATOR);
		if (part.length < 6) {
			return null;
		}
		try {
			String userName = part[0];
			String screenName = part[1];
			String location = part[2];
			LocalDate tweetAt = LocalDate.parse(part[3], CoronaTweeterRepository.formatter);
			String originalTweet = part[4];
			String sentiment = part[5];
			return CoronaTweeter.of(userName, screenName, location, tweetAt, originalTweet, sentiment);
		} catch (DateTimeParseException ex) {
			log.warn("DateTime:" + ex.getMessage());
		} catch (NumberFormatException ex) {
			log.warn("Number:" + ex.getMessage());
		}
		return null;
	}


	public static CoronaTweeter parseCo(Tuple2<String, String> tuple2) {
		return parseCo(tuple2._2());
	}

}
