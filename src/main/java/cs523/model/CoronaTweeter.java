package cs523.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
public class CoronaTweeter implements Serializable {

	private static final long serialVersionUID = 1L;

	private String userName;
	private String screenName;
	private String location;
	private LocalDateTime tweetAt;
	private String originalTweet;
	private String sentiment;

	public static CoronaTweeter of(String userName, String screenName, String location, LocalDateTime tweetAt, String originalTweet, String sentiment) {
		return new CoronaTweeter(userName, screenName, location, tweetAt, originalTweet, sentiment);
	}

}
