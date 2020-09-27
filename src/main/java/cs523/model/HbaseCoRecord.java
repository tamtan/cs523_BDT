package cs523.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

@Data
@AllArgsConstructor
public class HbaseCoRecord implements Serializable{
	private static final long serialVersionUID = 1L;

	private String userName;
	private String screenName;
	private String location;
	private String tweetAt;
	private String originalTweet;
	private String sentiment;
	public static HbaseCoRecord of(CoronaTweeter aq) {
		String userName = aq.getUserName();
		String screenName = aq.getScreenName();
		String location = aq.getLocation();
		String tweetAt = aq.getTweetAt().toString();
		String originalTweet = aq.getOriginalTweet();
		String sentiment = aq.getSentiment();
		return new HbaseCoRecord(userName, screenName, location, tweetAt, originalTweet, sentiment);
	}
}
