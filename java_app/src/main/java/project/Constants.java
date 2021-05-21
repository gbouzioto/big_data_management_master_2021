package project;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public final class Constants {

    private Constants() {
        // restrict instantiation
    }

    public static final int ID = 1;
    public static final int NAME = 2;
    public static final int SEX = 3;
    public static final int AGE = 4;
    public static final int HEIGHT = 5;
    public static final int WEIGHT = 6;
    public static final int TEAM = 7;
    public static final int NOC = 8;
    public static final int GAMES = 9;
    public static final int YEAR = 10;
    public static final int SEASON = 11;
    public static final int CITY = 12;
    public static final int SPORT = 13;
    public static final int EVENT = 14;
    public static final int MEDAL = 15;
    public final static Text GOLD = new Text("Gold");
    public final static Text SILVER = new Text("Silver");
    public final static Text BRONZE = new Text("Bronze");
    public final static IntWritable ONE = new IntWritable(1);
    public static final int MAX_TOP_ATHLETES = 10;
    public static final int MAX_TOP_TEAMS = 3;
    public static final Text FEMALE = new Text("F");
}