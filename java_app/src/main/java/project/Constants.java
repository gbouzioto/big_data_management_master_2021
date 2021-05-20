package project;

import org.apache.hadoop.io.Text;

public final class Constants {

    private Constants() {
        // restrict instantiation
    }

    public static final int ID = 0;
    public static final int NAME = 1;
    public static final int SEX = 2;
    public static final int AGE = 3;
    public static final int HEIGHT = 4;
    public static final int WEIGHT = 5;
    public static final int TEAM = 6;
    public static final int NOC = 7;
    public static final int GAMES = 8;
    public static final int YEAR = 9;
    public static final int SEASON = 10;
    public static final int CITY = 11;
    public static final int SPORT = 12;
    public static final int EVENT = 13;
    public static final int MEDAL = 14;
    public final static Text GOLD = new Text("Gold");
    public final static Text SILVER = new Text("Silver");
    public final static Text BRONZE = new Text("Bronze");
    public static final int MAX_TOP_ATHLETES = 10;
    public static final int MAX_TOP_TEAMS = 3;
    public static final Text FEMALE = new Text("F");
}